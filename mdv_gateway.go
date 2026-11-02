// MDV/Lessar RS485 -> Modbus TCP gateway.
// Go-порт Python 2.7 скрипта. Только стандартная библиотека.
// config.json лежит рядом с бинарником.
//
// Сборка на Windows под ARMv7:
//
//	set GOOS=linux
//	set GOARCH=arm
//	set GOARM=7
//	set CGO_ENABLED=0
//	go build -ldflags="-s -w" -o mdv_gateway_arm7 mdv_gateway.go
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// ==========================================
// Константы
// ==========================================

const (
	regsPerDevice = 10

	regPower       = 0
	regMode        = 1
	regSpeed       = 2
	regSetTemp     = 3
	regRoomTemp    = 4
	regOnline      = 5
	regError       = 6
	regControlMode = 7 // 0=AUTO, 1=MANUAL (обслуживание)

	noChange = 32768 // "не менять"

	controlAuto   = 0
	controlManual = 1

	protectionWindow   = 6 * time.Second
	minCommandInterval = 1500 * time.Millisecond
	warnInterval       = 60 * time.Second

	cbaud   = 0x100f
	crtscts = 0x80000000
	tcflsh  = 0x540b // ioctl TCFLSH

	// Таймауты RS485 (оптимизированы под 4800 бод)
	pollRespTimeout = 250 * time.Millisecond
	ctrlRespTimeout = 250 * time.Millisecond
	idleGap         = 25 * time.Millisecond // тишина = конец пакета

	serialRetryDelay = 5 * time.Second
	serialMaxBackoff = 20 * time.Second
	workerPanicDelay = 5 * time.Second
	pollWriteTimeout = 1 * time.Second
	maxRespBytes     = 64

	consecutiveErrLimit = 5  // сколько ошибок подряд = порт мёртв
	aliveCheckEvery     = 10 // проверять TCGETS раз в N циклов

	// Диапазон MDV-адресов. Верхняя граница условная — уточни
	// по документации конкретной серии (16/32/64/128).
	// Нижняя граница 0: адрес 0 используется в реальных конфигах.
	mdvAddrMin = 0
	mdvAddrMax = 64

	// Modbus spec: FC03 max 125 регистров, FC16 max 123 (byte count внутри PDU)
	modbusMaxReadRegs  = 125
	modbusMaxWriteRegs = 123

	maxFailedCommands = 3
	maxFailedPolls    = 5
)

var mdvModes = map[int]int{0: 8, 1: 4, 2: 2, 3: 1, 4: 16}
var mdvSpeeds = map[int]int{1: 4, 2: 2, 3: 1, 4: 128}
var modeMap = map[int]int{8: 0, 4: 1, 2: 2, 1: 3, 16: 4}
var speedMap = map[int]int{4: 1, 2: 2, 1: 3, 128: 4}

// ==========================================
// Логирование
// ==========================================

const (
	lvlDebug = iota
	lvlInfo
	lvlWarn
	lvlError
)

var levelNames = []string{"DEBUG", "INFO", "WARNING", "ERROR"}

var (
	logLevel           = lvlWarn
	logInfo            = false
	logDebug           = false
	logW     io.Writer = os.Stderr
	logMu    sync.Mutex
)

func setLogLevel(l int) {
	logLevel = l
	logInfo = l <= lvlInfo
	logDebug = l <= lvlDebug
}

func logf(level int, format string, args ...interface{}) {
	if level < logLevel {
		return
	}
	line := time.Now().Format("2006-01-02 15:04:05.000") + " [" + levelNames[level] + "] " +
		fmt.Sprintf(format, args...) + "\n"
	logMu.Lock()
	logW.Write([]byte(line))
	logMu.Unlock()

	if level >= lvlError && logW != os.Stderr {
		fmt.Fprint(os.Stderr, line)
	}
}

type wkey struct {
	name string
	addr int
}

var (
	warnMu   sync.Mutex
	warnLast = map[wkey]time.Time{}
)

func warnThrottled(key wkey, format string, args ...interface{}) {
	if lvlWarn < logLevel {
		return
	}
	warnMu.Lock()
	now := time.Now()
	last, ok := warnLast[key]
	if ok && now.Sub(last) < warnInterval {
		warnMu.Unlock()
		return
	}
	warnLast[key] = now
	warnMu.Unlock()
	logf(lvlWarn, format, args...)
}

// ==========================================
// Ротация лог-файла
// ==========================================

type rotWriter struct {
	mu      sync.Mutex
	path    string
	max     int64
	backups int
	f       *os.File
	size    int64
}

func newRotWriter(path string, max int64, backups int) (*rotWriter, error) {
	w := &rotWriter{path: path, max: max, backups: backups}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *rotWriter) open() error {
	f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.size = 0
	if st, err := f.Stat(); err == nil {
		w.size = st.Size()
	}
	w.f = f
	return nil
}

func (w *rotWriter) rotate() {
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
	for i := w.backups - 1; i >= 1; i-- {
		old := fmt.Sprintf("%s.%d", w.path, i)
		new := fmt.Sprintf("%s.%d", w.path, i+1)
		if err := os.Rename(old, new); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "log rotate rename %s -> %s: %v\n", old, new, err)
		}
	}
	if err := os.Rename(w.path, w.path+".1"); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "log rotate rename %s -> %s.1: %v\n", w.path, w.path, err)
	}
	if err := w.open(); err != nil {
		fmt.Fprintf(os.Stderr, "log reopen after rotate: %v\n", err)
	}
}

func (w *rotWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		if err := w.open(); err != nil {
			return 0, err
		}
	}
	if w.size+int64(len(p)) > w.max {
		w.rotate()
		if w.f == nil {
			return 0, errors.New("log file unavailable")
		}
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

func hexStr(data []byte) string {
	if len(data) == 0 {
		return "EMPTY"
	}
	const hx = "0123456789ABCDEF"
	out := make([]byte, 0, len(data)*3)
	for i, b := range data {
		if i > 0 {
			out = append(out, ' ')
		}
		out = append(out, hx[b>>4], hx[b&0x0F])
	}
	return string(out)
}

// ==========================================
// Конфиг
// ==========================================

type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	n, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*f = flexInt(n)
	return nil
}

type Config struct {
	Serial struct {
		Port     string  `json:"port"`
		Baudrate flexInt `json:"baudrate"`
	} `json:"serial"`
	Devices []struct {
		Address flexInt `json:"address"`
	} `json:"devices"`
	Modbus struct {
		Host string  `json:"host"`
		Port flexInt `json:"port"`
	} `json:"modbus"`
	LogLevel string `json:"log_level"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.Serial.Port == "" {
		return nil, errors.New("serial.port is empty")
	}
	if c.Modbus.Port == 0 {
		return nil, errors.New("modbus.port is empty")
	}
	if len(c.Devices) == 0 {
		return nil, errors.New("devices[] is empty")
	}

	// Валидация адресов: диапазон 0..64 + уникальность
	seen := make(map[int]bool, len(c.Devices))
	for i, dev := range c.Devices {
		addr := int(dev.Address)
		if addr < mdvAddrMin || addr > mdvAddrMax {
			return nil, fmt.Errorf("device[%d]: address %d out of range %d..%d",
				i, addr, mdvAddrMin, mdvAddrMax)
		}
		if seen[addr] {
			return nil, fmt.Errorf("device[%d]: duplicate address %d", i, addr)
		}
		seen[addr] = true
	}

	return &c, nil
}

// ==========================================
// Глобальное состояние
// ==========================================

var (
	registers []int
	regLock   sync.Mutex
	running   int32 = 1
)

func isRunning() bool { return atomic.LoadInt32(&running) == 1 }

// ==========================================
// 1. MDV PROTOCOL (RS485)
// ==========================================

func calcChecksum(pkt []byte) byte {
	s := 85
	for i := 1; i < 15; i++ {
		s += int(pkt[i])
	}
	return byte(255 - (s % 256))
}

func buildPollRequest(addr int) []byte {
	chk := byte((129 - addr) & 0xFF)
	return []byte{0xFE, 0xAA, 0xC0, byte(addr), 0x00, 0x80, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3F, chk, 0x55}
}

func buildControlRequest(addr, power, mode, speed, temp int) []byte {
	pkt := []byte{0xFE, 0xAA, 0xC3, byte(addr), 0x00, 0x80, 0x00, 0x08,
		0x80, 0x15, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x00, 0x55}

	pwrBit := 0
	if power != 0 {
		pwrBit = 0x80
	}
	modeBits, ok := mdvModes[mode]
	if !ok {
		modeBits = 8
	}
	pkt[7] = byte(pwrBit | modeBits)
	speedBits, ok := mdvSpeeds[speed]
	if !ok {
		speedBits = 128
	}
	pkt[8] = byte(speedBits)
	t := temp
	if t < 16 {
		t = 16
	}
	if t > 31 {
		t = 31
	}
	pkt[9] = byte(t)
	pkt[15] = calcChecksum(pkt)
	return pkt
}

type devState struct {
	power, mode, speed, setTemp, roomTemp int
}

// parseResponse проверяет:
//   - минимальную длину
//   - заголовок 0xAA
//   - команду (C0/C3)
//   - адрес (эхо байта 3)
//   - финальный байт 0x55
//
// Checksum ответа НЕ проверяется: формула не подтверждена
// для реального железа, рабочий Python-скрипт её тоже не проверяет.
func parseResponse(d []byte, expectedAddr int) *devState {
	if len(d) < 22 {
		return nil
	}
	if d[0] != 0xAA {
		return nil
	}
	cmd := d[1]
	if cmd != 0xC0 && cmd != 0xC3 {
		return nil
	}
	if int(d[3]) != expectedAddr {
		if logDebug {
			logf(lvlDebug, "PARSE: addr mismatch got=%d expected=%d", d[3], expectedAddr)
		}
		return nil
	}
	if d[len(d)-1] != 0x55 {
		if logDebug {
			logf(lvlDebug, "PARSE: bad tail 0x%02X", d[len(d)-1])
		}
		return nil
	}

	var flags, speedRaw int
	if cmd == 0xC3 {
		// Ответ на команду: состояние в байтах 18/19, темп. в 10/11 (эхо)
		flags = int(d[18])
		speedRaw = int(d[19])
	} else {
		// Ответ на опрос: состояние в байтах 8/9
		flags = int(d[8])
		speedRaw = int(d[9])
	}
	power := 0
	if flags&0x80 != 0 {
		power = 1
	}
	modeRaw := flags & 0x1F

	setTemp := int(d[10])
	if setTemp < 16 || setTemp > 31 {
		setTemp = 24
	}

	roomRaw := int(d[11])
	roomTemp := 0
	if roomRaw > 0 {
		roomTemp = roomRaw/2 - 20
	}
	if roomTemp < -20 || roomTemp > 60 {
		roomTemp = 0
	}

	mode, ok := modeMap[modeRaw]
	if !ok {
		mode = 0
	}
	speed, ok := speedMap[speedRaw]
	if !ok {
		speed = 4
	}

	if logDebug {
		logf(lvlDebug, "PARSE: cmd=0x%02X P=%d M=%d S=%d T=%d Room=%dC",
			cmd, power, mode, speed, setTemp, roomTemp)
	}

	return &devState{power, mode, speed, setTemp, roomTemp}
}

// ==========================================
// Последовательный порт
// ==========================================

var baudTable = map[int]uint32{
	300: syscall.B300, 600: syscall.B600, 1200: syscall.B1200, 2400: syscall.B2400,
	4800: syscall.B4800, 9600: syscall.B9600, 19200: syscall.B19200, 38400: syscall.B38400,
	57600: syscall.B57600, 115200: syscall.B115200, 230400: syscall.B230400,
	460800: syscall.B460800, 921600: syscall.B921600,
}

type serialPort struct {
	f  *os.File
	fd int
}

func (s *serialPort) ioctl(req uintptr, arg uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(s.fd), req, arg)
	if errno != 0 {
		return errno
	}
	return nil
}

func openSerial(path string, baud int) (*serialPort, error) {
	speed, ok := baudTable[baud]
	if !ok {
		return nil, fmt.Errorf("unsupported baudrate %d", baud)
	}

	// БЕЗ O_NONBLOCK — работаем через select(2)
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, err
	}

	s := &serialPort{f: f, fd: int(f.Fd())}

	var t syscall.Termios
	if err := s.ioctl(syscall.TCGETS, uintptr(unsafe.Pointer(&t))); err != nil {
		f.Close()
		return nil, err
	}

	t.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP |
		syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON | syscall.IXOFF |
		syscall.IXANY | syscall.INPCK
	t.Oflag &^= syscall.OPOST
	t.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ECHOE | syscall.ECHOK |
		syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	t.Cflag &^= syscall.CSIZE | syscall.PARENB | syscall.PARODD | syscall.CSTOPB |
		crtscts | cbaud
	t.Cflag |= syscall.CS8 | syscall.CREAD | syscall.CLOCAL | speed
	t.Ispeed = speed
	t.Ospeed = speed

	// VMIN/VTIME не используются — таймауты через select(2)
	t.Cc[syscall.VMIN] = 0
	t.Cc[syscall.VTIME] = 0

	if err := s.ioctl(syscall.TCSETS, uintptr(unsafe.Pointer(&t))); err != nil {
		f.Close()
		return nil, err
	}

	_ = s.flushInput()

	return s, nil
}

func (s *serialPort) flushInput() error {
	return s.ioctl(tcflsh, 0)
}

// alive проверяет, что fd ещё ассоциирован с устройством.
func (s *serialPort) alive() bool {
	var t syscall.Termios
	return s.ioctl(syscall.TCGETS, uintptr(unsafe.Pointer(&t))) == nil
}

// isFatalSerialErr — ошибки, после которых порт надо переоткрыть.
func isFatalSerialErr(err error) bool {
	return err == syscall.ENODEV || err == syscall.EIO ||
		err == syscall.EBADF || err == syscall.EPIPE ||
		err == syscall.ENXIO
}

// write: блокирующая запись через syscall.Write с select на запись.
func (s *serialPort) write(b []byte) error {
	deadline := time.Now().Add(pollWriteTimeout)
	for len(b) > 0 {
		n, err := syscall.Write(s.fd, b)
		if n > 0 {
			b = b[n:]
		}
		if err == nil {
			continue
		}
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			if time.Now().After(deadline) {
				return errors.New("write timeout")
			}
			var wfds syscall.FdSet
			wfds.Bits[s.fd/64] |= 1 << (uint(s.fd) % 64)
			tv := syscall.Timeval{Sec: 0, Usec: 100000}
			_, _ = syscall.Select(s.fd+1, nil, &wfds, nil, &tv)
			continue
		}
		if err == syscall.EINTR {
			continue
		}
		return err
	}
	return nil
}

// readUntilIdle читает до max байт, завершаясь когда линия молчит idle
// или истёк общий total-таймаут. Критично для скорости на 4800 бод.
func (s *serialPort) readUntilIdle(max int, idle time.Duration, total time.Duration) ([]byte, error) {
	buf := make([]byte, 0, max)
	deadline := time.Now().Add(total)

	for len(buf) < max {
		wait := idle
		if rem := time.Until(deadline); rem < wait {
			wait = rem
		}
		if wait <= 0 {
			break
		}

		var rfds syscall.FdSet
		rfds.Bits[s.fd/64] |= 1 << (uint(s.fd) % 64)
		tv := syscall.NsecToTimeval(wait.Nanoseconds())

		nReady, err := syscall.Select(s.fd+1, &rfds, nil, nil, &tv)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return buf, err
		}
		if nReady == 0 {
			if len(buf) > 0 {
				break
			}
			if time.Now().After(deadline) {
				break
			}
			continue
		}

		chunk := make([]byte, max-len(buf))
		k, err := syscall.Read(s.fd, chunk)
		if k > 0 {
			buf = append(buf, chunk[:k]...)
		}
		if err != nil {
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK || err == syscall.EINTR {
				continue
			}
			if err == io.EOF {
				break
			}
			return buf, err
		}
	}
	return buf, nil
}

func (s *serialPort) close() error {
	return s.f.Close()
}

// ==========================================
// 2. MODBUS TCP SERVER
// ==========================================
//
// Регистр 40008 (regControlMode):
//   0 = AUTO   — обычная работа со SCADA
//   1 = MANUAL — обслуживание: C3 от SCADA игнорируется,
//                C0 polling / ROOM / ONLINE / ERROR продолжаются.
//

func isValidControlValue(regOffset, value int) (bool, int, bool) {
	if regOffset == regControlMode {
		return value == controlAuto || value == controlManual, value, true
	}
	if regOffset >= regRoomTemp {
		return false, 0, false
	}
	if value == noChange {
		return true, 0, false
	}
	switch regOffset {
	case regPower:
		return value == 0 || value == 1, value, true
	case regMode:
		return value >= 0 && value <= 4, value, true
	case regSpeed:
		return value >= 1 && value <= 4, value, true
	case regSetTemp:
		return value >= 16 && value <= 31, value, true
	}
	return true, value, true
}

// isManualRegisterLocked вызывается только когда regLock уже взят.
func isManualRegisterLocked(idx int) bool {
	if idx < 0 || idx >= len(registers) {
		return false
	}
	base := (idx / regsPerDevice) * regsPerDevice
	return registers[base+regControlMode] == controlManual
}

func regIndexOf(addr int) (idx int, mbAddr int) {
	if addr >= 40000 {
		return addr - 40001, addr
	}
	return addr, addr + 40001
}

// modbusWriteHook — уведомляет воркер, что регистры изменил внешний клиент.
var modbusWriteHook atomic.Value // func(devIndex int)

func processPDU(pdu []byte) (resp []byte, ok bool) {
	if len(pdu) < 1 {
		return nil, false
	}
	fc := int(pdu[0])

	var touchedAddrs []int // индексы устройств, изменённых внешним клиентом

	regLock.Lock()

	switch fc {
	case 3:
		if len(pdu) < 5 {
			regLock.Unlock()
			return nil, false
		}
		start := int(binary.BigEndian.Uint16(pdu[1:3]))
		qty := int(binary.BigEndian.Uint16(pdu[3:5]))
		idx, mbAddr := regIndexOf(start)

		if logInfo {
			logf(lvlInfo, "READ: %d (idx=%d), count=%d", mbAddr, idx, qty)
		}

		// Modbus spec: FC03 max 125 регистров
		if qty < 1 || qty > modbusMaxReadRegs {
			regLock.Unlock()
			logf(lvlWarn, "READ invalid qty: %d", qty)
			return []byte{byte(fc + 0x80), 0x03}, true // illegal data value
		}

		if idx >= 0 && idx+qty <= len(registers) {
			data := make([]byte, 0, qty*2)
			var values []string
			for i := idx; i < idx+qty; i++ {
				v := registers[i]
				data = append(data, byte(v>>8), byte(v))
				if logInfo {
					values = append(values, strconv.Itoa(v))
				}
			}
			resp = append([]byte{byte(fc), byte(len(data))}, data...)
			if logInfo {
				logf(lvlInfo, "DATA: %s", strings.Join(values, ", "))
			}
			regLock.Unlock()
			return resp, true
		}
		regLock.Unlock()
		logf(lvlWarn, "READ out of range: idx=%d", idx)
		return []byte{byte(fc + 0x80), 0x02}, true

	case 6:
		if len(pdu) < 5 {
			regLock.Unlock()
			return nil, false
		}
		regAddr := int(binary.BigEndian.Uint16(pdu[1:3]))
		value := int(binary.BigEndian.Uint16(pdu[3:5]))
		idx, mbAddr := regIndexOf(regAddr)

		if logInfo {
			logf(lvlInfo, "WRITE: %d (idx=%d) = %d", mbAddr, idx, value)
		}

		if idx >= 0 && idx < len(registers) {
			oldValue := registers[idx]
			regOffset := idx % regsPerDevice

			// В MANUAL записи POWER/MODE/SPEED/TEMP от SCADA
			// не меняют внутренние команды и не уходят на AC.
			// Сам regControlMode (40008) всегда разрешён — через него
			// выполняется возврат в AUTO.
			if regOffset >= regPower && regOffset <= regSetTemp && isManualRegisterLocked(idx) {
				if logInfo {
					logf(lvlInfo, "WRITE IGNORED: AC %d is in MANUAL mode", idx/regsPerDevice)
				}
				resp = []byte{byte(fc), pdu[1], pdu[2], pdu[3], pdu[4]}
				regLock.Unlock()
				return resp, true
			}

			valid, newValue, has := isValidControlValue(regOffset, value)
			if valid {
				if !has {
					if logInfo {
						logf(lvlInfo, "WRITE NOCHANGE: idx=%d (keeping %d)", idx, oldValue)
					}
				} else {
					registers[idx] = newValue
					touchedAddrs = append(touchedAddrs, idx/regsPerDevice)
					if logInfo {
						logf(lvlInfo, "WRITE OK: idx=%d %d -> %d", idx, oldValue, newValue)
					}
				}
			} else {
				logf(lvlWarn, "WRITE INVALID: idx=%d value=%d (ignored)", idx, value)
			}
			resp = []byte{byte(fc), pdu[1], pdu[2], pdu[3], pdu[4]}
			regLock.Unlock()
			notifyModbusWrite(touchedAddrs)
			return resp, true
		}
		regLock.Unlock()
		logf(lvlWarn, "WRITE out of range: idx=%d", idx)
		return []byte{byte(fc + 0x80), 0x02}, true

	case 16:
		if len(pdu) < 6 {
			regLock.Unlock()
			return nil, false
		}
		start := int(binary.BigEndian.Uint16(pdu[1:3]))
		qty := int(binary.BigEndian.Uint16(pdu[3:5]))
		idx, mbAddr := regIndexOf(start)

		if logInfo {
			logf(lvlInfo, "WRITE MULTI: %d (idx=%d), count=%d", mbAddr, idx, qty)
		}

		// Modbus spec: FC16 max 123 регистра (byte count внутри PDU)
		if qty < 1 || qty > modbusMaxWriteRegs {
			regLock.Unlock()
			logf(lvlWarn, "WRITE MULTI invalid qty: %d", qty)
			return []byte{byte(fc + 0x80), 0x03}, true
		}

		if idx >= 0 && idx+qty <= len(registers) {
			changes := 0
			for i := 0; i < qty; i++ {
				if 8+i*2 > len(pdu) {
					regLock.Unlock()
					return nil, false
				}
				val := int(binary.BigEndian.Uint16(pdu[6+i*2 : 8+i*2]))
				regOffset := (idx + i) % regsPerDevice
				old := registers[idx+i]

				valid, newValue, has := isValidControlValue(regOffset, val)
				if valid {
					if has {
						registers[idx+i] = newValue
						touchedAddrs = append(touchedAddrs, (idx+i)/regsPerDevice)
						changes++
						if logInfo {
							logf(lvlInfo, "  reg %d: %d -> %d", idx+i, old, newValue)
						}
					} else {
						if logInfo {
							logf(lvlInfo, "  reg %d: NOCHANGE (keeping %d)", idx+i, old)
						}
					}
				} else {
					logf(lvlWarn, "  reg %d: invalid value %d (skipped)", idx+i, val)
				}
			}
			if logInfo {
				logf(lvlInfo, "WRITE MULTI OK: %d of %d regs changed", changes, qty)
			}
			resp = []byte{byte(fc), pdu[1], pdu[2], pdu[3], pdu[4]}
			regLock.Unlock()
			notifyModbusWrite(touchedAddrs)
			return resp, true
		}
		regLock.Unlock()
		logf(lvlWarn, "WRITE MULTI out of range")
		return []byte{byte(fc + 0x80), 0x02}, true
	}

	regLock.Unlock()

	logf(lvlWarn, "Unsupported FC: %d", fc)
	if fc+0x80 > 255 {
		return nil, false
	}
	return []byte{byte(fc + 0x80), 0x01}, true
}

func notifyModbusWrite(devIndexes []int) {
	if len(devIndexes) == 0 {
		return
	}
	fn, _ := modbusWriteHook.Load().(func(int))
	if fn == nil {
		return
	}
	for _, i := range devIndexes {
		fn(i)
	}
}

func handleModbusClient(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			logf(lvlError, "Modbus client panic: %v", r)
			logf(lvlError, "%s", debug.Stack())
		}
		conn.Close()
		if logInfo {
			logf(lvlInfo, "Connection closed")
		}
	}()

	hdr := make([]byte, 7)
	for isRunning() {
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return
		}
		transID := binary.BigEndian.Uint16(hdr[0:2])
		protoID := binary.BigEndian.Uint16(hdr[2:4])
		length := int(binary.BigEndian.Uint16(hdr[4:6]))
		unitID := hdr[6]

		if length < 2 {
			return
		}
		pdu := make([]byte, length-1)
		if _, err := io.ReadFull(conn, pdu); err != nil {
			return
		}

		respPDU, ok := processPDU(pdu)
		if !ok {
			return
		}

		out := make([]byte, 7+len(respPDU))
		binary.BigEndian.PutUint16(out[0:2], transID)
		binary.BigEndian.PutUint16(out[2:4], protoID)
		binary.BigEndian.PutUint16(out[4:6], uint16(len(respPDU)+1))
		out[6] = unitID
		copy(out[7:], respPDU)
		if _, err := conn.Write(out); err != nil {
			return
		}

		if logDebug {
			logf(lvlDebug, "Response sent (len=%d)", len(out))
		}
	}
}

func startModbusServer(host string, port int) {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		msg := fmt.Sprintf("Modbus TCP server error: %v", err)
		logf(lvlError, "%s", msg)
		fmt.Fprintln(os.Stderr, msg)
		return
	}
	defer ln.Close()

	logf(lvlInfo, "%s", strings.Repeat("=", 60))
	logf(lvlInfo, "Modbus TCP server started!")
	logf(lvlInfo, "   IP: %s", host)
	logf(lvlInfo, "   Port: %d", port)
	logf(lvlInfo, "   Registers per device: %d", regsPerDevice)
	logf(lvlInfo, "   Devices configured: %d", len(registers)/regsPerDevice)
	logf(lvlInfo, "%s", strings.Repeat("=", 60))

	for isRunning() {
		conn, err := ln.Accept()
		if err != nil {
			if isRunning() {
				logf(lvlError, "Accept error: %v", err)
			}
			time.Sleep(time.Second)
			continue
		}
		if logInfo {
			logf(lvlInfo, "New connection from %s", conn.RemoteAddr().String())
		}
		go handleModbusClient(conn)
	}
}

// ==========================================
// 3. RS485 POLLING LOOP
// ==========================================

type cmdState struct {
	power, mode, speed, temp int
}

type worker struct {
	cfg *Config
	ser *serialPort

	blocks             map[int]int
	lastWriteTime      map[int]time.Time
	lastCmdState       map[int]*cmdState
	lastSuccessfulPoll map[int]time.Time
	initialized        map[int]bool
	pendingExternal    map[int]*cmdState

	// Ручной режим обслуживания.
	// manualMode — фактический режим worker'а.
	// manualSavedState — последнее состояние, которое SCADA задавала
	// до перехода в MANUAL; именно его восстанавливаем при возврате в AUTO.
	manualMode       map[int]bool
	manualSavedState map[int]*cmdState

	// РАЗДЕЛЁННЫЕ счётчики:
	failedCommands map[int]int // только ошибки CONTROL
	failedPolls    map[int]int // только ошибки POLL

	// Generation counter — защита от гонок и сброс give-up.
	genMu       sync.Mutex
	gen         map[int]uint64
	genAtGiveUp map[int]uint64 // зафиксированное поколение в момент "сдались"

	// Счётчики подряд идущих ошибок порта
	consecutivePollErrors  int
	consecutiveWriteErrors int

	cycleCount int
}

func newWorker(cfg *Config) *worker {
	w := &worker{
		cfg:                cfg,
		blocks:             map[int]int{},
		lastWriteTime:      map[int]time.Time{},
		lastCmdState:       map[int]*cmdState{},
		lastSuccessfulPoll: map[int]time.Time{},
		initialized:        map[int]bool{},
		pendingExternal:    map[int]*cmdState{},
		manualMode:         map[int]bool{},
		manualSavedState:   map[int]*cmdState{},
		failedCommands:     map[int]int{},
		failedPolls:        map[int]int{},
		gen:                map[int]uint64{},
		genAtGiveUp:        map[int]uint64{},
	}
	for index, dev := range cfg.Devices {
		addr := int(dev.Address)
		w.blocks[addr] = index * regsPerDevice
		w.lastWriteTime[addr] = time.Time{}
		w.lastCmdState[addr] = nil
		w.lastSuccessfulPoll[addr] = time.Time{}
		w.failedCommands[addr] = 0
		w.failedPolls[addr] = 0
		w.pendingExternal[addr] = nil
		w.manualMode[addr] = false
		w.manualSavedState[addr] = nil
		w.gen[addr] = 0
		w.genAtGiveUp[addr] = 0
	}
	// Modbus-сервер уведомляет воркер о записях (по индексу устройства).
	modbusWriteHook.Store(func(devIndex int) {
		if devIndex < 0 || devIndex >= len(cfg.Devices) {
			return
		}
		addr := int(cfg.Devices[devIndex].Address)
		w.bumpGen(addr)
	})
	return w
}

func (w *worker) bumpGen(addr int) {
	w.genMu.Lock()
	w.gen[addr]++
	w.genMu.Unlock()
}

func (w *worker) getGen(addr int) uint64 {
	w.genMu.Lock()
	v := w.gen[addr]
	w.genMu.Unlock()
	return v
}

func validCmdState(s *cmdState) bool {
	if s == nil {
		return false
	}
	return (s.power == 0 || s.power == 1) &&
		s.mode >= 0 && s.mode <= 4 &&
		s.speed >= 1 && s.speed <= 4 &&
		s.temp >= 16 && s.temp <= 31
}

func (w *worker) isManual(addr int) bool {
	return w.manualMode[addr]
}

// syncControlModes обрабатывает переходы AUTO <-> MANUAL до отправки
// очередной команды. В MANUAL значения POWER/MODE/SPEED/TEMP из SCADA
// не отправляются на AC, а polling и ONLINE/ROOM/ERROR продолжают работать.
// При возврате в AUTO восстанавливается последнее состояние SCADA,
// сохранённое перед входом в MANUAL.
func (w *worker) syncControlModes() {
	for _, dev := range w.cfg.Devices {
		if !isRunning() {
			return
		}

		addr := int(dev.Address)
		base := w.blocks[addr]

		regLock.Lock()
		manualRequested := registers[base+regControlMode] == controlManual
		currentManual := w.manualMode[addr]

		if manualRequested == currentManual {
			regLock.Unlock()
			continue
		}

		if manualRequested {
			saved := &cmdState{
				power: registers[base+regPower],
				mode:  registers[base+regMode],
				speed: registers[base+regSpeed],
				temp:  registers[base+regSetTemp],
			}
			if !validCmdState(saved) {
				saved = nil
			}

			w.manualMode[addr] = true
			w.manualSavedState[addr] = saved
			w.pendingExternal[addr] = nil
			w.failedCommands[addr] = 0
			regLock.Unlock()

			if logInfo {
				if saved != nil {
					logf(lvlInfo, "AC %d: MANUAL mode enabled, resume state saved (P:%d M:%d S:%d T:%d)",
						addr, saved.power, saved.mode, saved.speed, saved.temp)
				} else {
					logf(lvlInfo, "AC %d: MANUAL mode enabled (resume state not available yet)", addr)
				}
			}

			continue
		}

		// MANUAL -> AUTO. До восстановления saved-state текущие регистры
		// 0..3 считаем фактическим состоянием AC после обслуживания.
		actual := &cmdState{
			power: registers[base+regPower],
			mode:  registers[base+regMode],
			speed: registers[base+regSpeed],
			temp:  registers[base+regSetTemp],
		}
		saved := w.manualSavedState[addr]

		w.manualMode[addr] = false
		w.pendingExternal[addr] = nil
		w.failedCommands[addr] = 0
		w.lastWriteTime[addr] = time.Time{}
		w.lastCmdState[addr] = actual
		if saved != nil {
			registers[base+regPower] = saved.power
			registers[base+regMode] = saved.mode
			registers[base+regSpeed] = saved.speed
			registers[base+regSetTemp] = saved.temp
		}
		w.manualSavedState[addr] = nil
		regLock.Unlock()

		if logInfo {
			if saved != nil {
				logf(lvlInfo, "AC %d: AUTO mode enabled, restoring SCADA state (P:%d M:%d S:%d T:%d)",
					addr, saved.power, saved.mode, saved.speed, saved.temp)
			} else {
				logf(lvlInfo, "AC %d: AUTO mode enabled", addr)
			}
		}
	}
}

func (w *worker) closeSerial() {
	if w.ser != nil {
		if err := w.ser.close(); err != nil {
			logf(lvlWarn, "Error closing serial port: %v", err)
		} else {
			logf(lvlInfo, "Serial port closed")
		}
		w.ser = nil
	}
}

// markSerialDead — сбрасывает порт, чтобы run() переоткрыл его.
func (w *worker) markSerialDead() {
	if w.ser != nil {
		w.ser.close()
		w.ser = nil
	}
	w.initialized = map[int]bool{}
	w.consecutivePollErrors = 0
	w.consecutiveWriteErrors = 0
}

func (w *worker) openSerial() bool {
	// Если порта нет в /dev — не пытаемся открыть.
	if _, err := os.Stat(w.cfg.Serial.Port); err != nil {
		warnThrottled(wkey{"port_missing", 0}, "Serial port %s not present: %v",
			w.cfg.Serial.Port, err)
		return false
	}

	for attempt := 0; attempt < 3; attempt++ {
		ser, err := openSerial(w.cfg.Serial.Port, int(w.cfg.Serial.Baudrate))
		if err == nil {
			w.ser = ser
			logf(lvlInfo, "Serial port %s opened (baud: %d)",
				w.cfg.Serial.Port, int(w.cfg.Serial.Baudrate))
			return true
		}
		if errors.Is(err, syscall.EBUSY) {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		warnThrottled(wkey{"open_serial", 0}, "Cannot open serial port: %v", err)
		return false
	}
	warnThrottled(wkey{"open_serial_busy", 0}, "Serial port busy after 3 attempts")
	return false
}

func (w *worker) pollDevice(addr int) *devState {
	ser := w.ser
	if ser == nil {
		return nil
	}

	_ = ser.flushInput()

	pkt := buildPollRequest(addr)
	if logDebug {
		logf(lvlDebug, "AC %d: SEND POLL: %s", addr, hexStr(pkt))
	}

	if err := ser.write(pkt); err != nil {
		if isFatalSerialErr(err) {
			logf(lvlError, "AC %d: fatal poll write error: %v", addr, err)
			w.markSerialDead()
			return nil
		}
		w.consecutiveWriteErrors++
		warnThrottled(wkey{"poll_write_err", addr}, "Poll write error for AC %d: %v", addr, err)
		return nil
	}
	w.consecutiveWriteErrors = 0

	start := time.Now()
	resp, err := ser.readUntilIdle(maxRespBytes, idleGap, pollRespTimeout)
	elapsed := time.Since(start)

	if err != nil {
		if isFatalSerialErr(err) {
			logf(lvlError, "AC %d: fatal poll read error: %v", addr, err)
			w.markSerialDead()
			return nil
		}
		w.consecutivePollErrors++
		warnThrottled(wkey{"poll_read_err", addr}, "Poll read error for AC %d: %v", addr, err)
		if w.consecutivePollErrors >= consecutiveErrLimit {
			logf(lvlWarn, "%d consecutive poll errors, reopening serial", w.consecutivePollErrors)
			w.markSerialDead()
		}
		return nil
	}

	if len(resp) == 0 {
		w.consecutivePollErrors++
		if w.consecutivePollErrors >= consecutiveErrLimit {
			logf(lvlWarn, "%d consecutive empty responses, reopening serial", w.consecutivePollErrors)
			w.markSerialDead()
		}
		return nil
	}
	w.consecutivePollErrors = 0

	if logDebug {
		logf(lvlDebug, "AC %d: RECV in %d ms (%d bytes): %s",
			addr, elapsed.Milliseconds(), len(resp), hexStr(resp))
	}

	return parseResponse(resp, addr)
}

func (w *worker) sendControl(addr, power, mode, speed, temp int) bool {
	ser := w.ser
	if ser == nil {
		return false
	}

	pkt := buildControlRequest(addr, power, mode, speed, temp)
	if logInfo {
		logf(lvlInfo, "AC %d: SEND CTRL: P=%d M=%d S=%d T=%d", addr, power, mode, speed, temp)
	}
	if logDebug {
		logf(lvlDebug, "AC %d: CTRL HEX: %s", addr, hexStr(pkt))
	}

	_ = ser.flushInput()

	if err := ser.write(pkt); err != nil {
		if isFatalSerialErr(err) {
			logf(lvlError, "AC %d: fatal ctrl write error: %v", addr, err)
			w.markSerialDead()
			return false
		}
		logf(lvlError, "Control write error for AC %d: %v", addr, err)
		w.failedCommands[addr]++
		return false
	}

	resp, err := ser.readUntilIdle(maxRespBytes, idleGap, ctrlRespTimeout)
	if err != nil {
		if isFatalSerialErr(err) {
			logf(lvlError, "AC %d: fatal ctrl read error: %v", addr, err)
			w.markSerialDead()
			return false
		}
		logf(lvlError, "Control read error for AC %d: %v", addr, err)
		w.failedCommands[addr]++
		return false
	}
	if logDebug && len(resp) > 0 {
		logf(lvlDebug, "AC %d: CTRL RESP: %s", addr, hexStr(resp))
	}

	ok := len(resp) >= 2 && resp[0] == 0xAA && resp[1] == 0xC3 &&
		(len(resp) < 4 || int(resp[3]) == addr)
	if ok {
		if logInfo {
			logf(lvlInfo, "AC %d: Control ACK received", addr)
		}
		w.failedCommands[addr] = 0
		// НЕ делаем verify-poll: следующий cycle() → pollOne() обновит состояние.
		return true
	}

	logf(lvlWarn, "AC %d: Control NO ACK (%d bytes)", addr, len(resp))
	w.failedCommands[addr]++
	return false
}

func (w *worker) updateDeviceState(addr int, s *devState) {
	base := w.blocks[addr]
	regLock.Lock()
	registers[base+regPower] = s.power
	registers[base+regMode] = s.mode
	registers[base+regSpeed] = s.speed
	registers[base+regSetTemp] = s.setTemp
	registers[base+regRoomTemp] = s.roomTemp
	registers[base+regOnline] = 1
	registers[base+regError] = 0
	regLock.Unlock()
}

func (w *worker) setRegister(idx, val int) {
	regLock.Lock()
	registers[idx] = val
	regLock.Unlock()
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// hasPendingCommand — дешёвая проверка расхождения.
func (w *worker) hasPendingCommand(addr int) bool {
	if w.isManual(addr) {
		return false
	}
	base := w.blocks[addr]
	last := w.lastCmdState[addr]
	if last == nil {
		return false
	}
	regLock.Lock()
	p := registers[base+regPower]
	m := registers[base+regMode]
	s := registers[base+regSpeed]
	t := registers[base+regSetTemp]
	regLock.Unlock()
	return p != last.power || m != last.mode || s != last.speed || t != last.temp
}

func (w *worker) checkAndSendCommand(addr int) bool {
	base := w.blocks[addr]

	// В MANUAL никаких C3 от SCADA не отправляем. Polling продолжает работать.
	if w.isManual(addr) {
		return false
	}

	if time.Since(w.lastSuccessfulPoll[addr]) > 30*time.Second {
		warnThrottled(wkey{"no_resp_30", addr}, "AC %d: No response for 30s, suppressing commands", addr)
		return false
	}

	// Подавление — ТОЛЬКО по ошибкам команд (failedCommands).
	// НО: если после give-up был внешний write (generation изменилась),
	// даём шанс новой команде.
	if w.failedCommands[addr] >= maxFailedCommands {
		if w.getGen(addr) == w.genAtGiveUp[addr] {
			warnThrottled(wkey{"too_many_fail", addr},
				"AC %d: Too many failed commands (%d), suppressing",
				addr, w.failedCommands[addr])
			return false
		}
		// Внешний write после give-up — сбрасываем счётчик
		if logInfo {
			logf(lvlInfo, "AC %d: new external command after give-up, reset counter", addr)
		}
		w.failedCommands[addr] = 0
		w.genAtGiveUp[addr] = w.getGen(addr)
	}

	if time.Since(w.lastWriteTime[addr]) < minCommandInterval {
		return false
	}

	regLock.Lock()
	mbPower := registers[base+regPower]
	mbMode := registers[base+regMode]
	mbSpeed := registers[base+regSpeed]
	mbTemp := registers[base+regSetTemp]
	regLock.Unlock()

	last := w.lastCmdState[addr]
	if last == nil {
		return false
	}

	needsWrite := false
	var changes []string

	if mbPower != last.power {
		needsWrite = true
		changes = append(changes, fmt.Sprintf("POWER: %d->%d", last.power, mbPower))
	}
	if mbMode != last.mode {
		needsWrite = true
		changes = append(changes, fmt.Sprintf("MODE: %d->%d", last.mode, mbMode))
	}
	if mbSpeed != last.speed {
		needsWrite = true
		changes = append(changes, fmt.Sprintf("SPEED: %d->%d", last.speed, mbSpeed))
	}
	if mbTemp != last.temp {
		needsWrite = true
		changes = append(changes, fmt.Sprintf("TEMP: %d->%d", last.temp, mbTemp))
	}

	if !needsWrite {
		return false
	}

	if logInfo {
		logf(lvlInfo, "AC %d: Changes detected: %s", addr, strings.Join(changes, ", "))
	}

	mbPowerI := b2i(mbPower != 0)

	if _, ok := mdvModes[mbMode]; !ok {
		logf(lvlWarn, "AC %d: invalid mode %d, reverting to %d", addr, mbMode, last.mode)
		w.setRegister(base+regMode, last.mode)
		return false
	}

	if _, ok := mdvSpeeds[mbSpeed]; !ok {
		logf(lvlWarn, "AC %d: invalid speed %d, reverting to %d", addr, mbSpeed, last.speed)
		w.setRegister(base+regSpeed, last.speed)
		return false
	}

	if mbTemp < 16 || mbTemp > 31 {
		logf(lvlWarn, "AC %d: invalid temp %d, reverting to %d", addr, mbTemp, last.temp)
		w.setRegister(base+regSetTemp, last.temp)
		return false
	}

	genBefore := w.getGen(addr)

	if w.sendControl(addr, mbPowerI, mbMode, mbSpeed, mbTemp) {
		w.lastCmdState[addr] = &cmdState{mbPowerI, mbMode, mbSpeed, mbTemp}
		w.lastWriteTime[addr] = time.Now()
		return true
	}

	w.lastWriteTime[addr] = time.Now()

	if w.getGen(addr) != genBefore {
		// Пользователь записал новое значение во время sendControl —
		// не откатываем, пусть следующий цикл разберётся.
		if logInfo {
			logf(lvlInfo, "AC %d: external write during control, skip rollback", addr)
		}
		return false
	}

	if w.failedCommands[addr] >= maxFailedCommands {
		logf(lvlWarn, "AC %d: Command failed %d times, giving up",
			addr, w.failedCommands[addr])
		// Запоминаем поколение на момент give-up,
		// чтобы при новом внешнем write разрешить попытки.
		w.genAtGiveUp[addr] = w.getGen(addr)

		regLock.Lock()
		registers[base+regPower] = last.power
		registers[base+regMode] = last.mode
		registers[base+regSpeed] = last.speed
		registers[base+regSetTemp] = last.temp
		regLock.Unlock()
	} else {
		if logInfo {
			logf(lvlInfo, "AC %d: Command failed, will retry (attempt %d/%d)",
				addr, w.failedCommands[addr], maxFailedCommands)
		}
	}
	return false
}

// handlePollResult — вызывается после pollDevice.
// state == nil → неудачный poll (счётчик failedPolls++).
// state != nil → успешный poll (failedPolls = 0, online = 1).
func (w *worker) handlePollResult(addr int, state *devState) {
	base := w.blocks[addr]

	if state == nil {
		w.failedPolls[addr]++

		// Offline — вариант B: online=0 только после maxFailedPolls подряд.
		if w.failedPolls[addr] >= maxFailedPolls {
			regLock.Lock()
			registers[base+regOnline] = 0
			regLock.Unlock()
		}
		if w.failedPolls[addr] == maxFailedPolls {
			logf(lvlWarn, "AC %d: offline after %d failed polls", addr, w.failedPolls[addr])
		}
		warnThrottled(wkey{"no_resp", addr}, "AC %d: No response", addr)
		return
	}

	// Успешный poll: сбрасываем ТОЛЬКО failedPolls.
	w.lastSuccessfulPoll[addr] = time.Now()
	w.failedPolls[addr] = 0

	// MANUAL: физическое состояние продолжаем показывать в регистрах,
	// но не синхронизируем его обратно с последним состоянием SCADA.
	if w.isManual(addr) {
		regLock.Lock()
		registers[base+regPower] = state.power
		registers[base+regMode] = state.mode
		registers[base+regSpeed] = state.speed
		registers[base+regSetTemp] = state.setTemp
		registers[base+regRoomTemp] = state.roomTemp
		registers[base+regOnline] = 1
		registers[base+regError] = 0
		regLock.Unlock()
		w.pendingExternal[addr] = nil
		return
	}

	logExternal := false
	confirmed := false

	regLock.Lock()

	last := w.lastCmdState[addr]

	if time.Since(w.lastWriteTime[addr]) > protectionWindow {
		cand := &cmdState{state.power, state.mode, state.speed, state.setTemp}

		if last == nil || *cand != *last {
			pending := w.pendingExternal[addr]
			if pending != nil && *pending == *cand {
				confirmed = true
				w.lastCmdState[addr] = cand
				registers[base+regPower] = state.power
				registers[base+regMode] = state.mode
				registers[base+regSpeed] = state.speed
				registers[base+regSetTemp] = state.setTemp
				w.pendingExternal[addr] = nil
			} else {
				logExternal = true
				w.pendingExternal[addr] = cand
			}
		} else {
			w.pendingExternal[addr] = nil
		}
	}

	registers[base+regRoomTemp] = state.roomTemp
	registers[base+regOnline] = 1
	registers[base+regError] = 0

	regLock.Unlock()

	if logInfo {
		if confirmed {
			logf(lvlInfo, "AC %d: External change CONFIRMED", addr)
		} else if logExternal {
			logf(lvlDebug, "AC %d: External change pending", addr)
		}
	}
	if logDebug {
		logf(lvlDebug, "AC %d: OK (Room:%dC, P:%d M:%d S:%d T:%d)",
			addr, state.roomTemp, state.power, state.mode, state.speed, state.setTemp)
	}
}

// pollOne — обёртка с recover, чтобы паника на одном устройстве
// не срывала опрос остальных.
func (w *worker) pollOne(addr int) {
	defer func() {
		if r := recover(); r != nil {
			logf(lvlError, "Panic polling AC %d: %v", addr, r)
			logf(lvlError, "%s", debug.Stack())
		}
	}()

	base := w.blocks[addr]

	if !w.initialized[addr] {
		state := w.pollDevice(addr)
		if state != nil {
			w.updateDeviceState(addr, state)
			w.lastCmdState[addr] = &cmdState{state.power, state.mode, state.speed, state.setTemp}
			if w.manualMode[addr] && w.manualSavedState[addr] == nil {
				w.manualSavedState[addr] = &cmdState{state.power, state.mode, state.speed, state.setTemp}
			}
			w.lastSuccessfulPoll[addr] = time.Now()
			w.failedPolls[addr] = 0
			w.failedCommands[addr] = 0
			w.initialized[addr] = true
			logf(lvlInfo, "AC %d: INITIALIZED (P:%d M:%d S:%d Set:%d Room:%dC)",
				addr, state.power, state.mode, state.speed, state.setTemp, state.roomTemp)
		} else {
			w.setRegister(base+regOnline, 0)
			warnThrottled(wkey{"no_resp_init", addr}, "AC %d: No response during init", addr)
		}
		return
	}

	if time.Since(w.lastSuccessfulPoll[addr]) < 30*time.Second {
		w.checkAndSendCommand(addr)
	}

	state := w.pollDevice(addr)
	w.handlePollResult(addr, state)
}

func (w *worker) cycle() (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			logf(lvlError, "RS485 worker error: %v", r)
			logf(lvlError, "%s", debug.Stack())
			ok = false
		}
	}()

	// Периодическая проверка живости порта.
	w.cycleCount++
	if w.cycleCount%aliveCheckEvery == 0 && w.ser != nil {
		if !w.ser.alive() {
			logf(lvlWarn, "Serial port is dead (TCGETS failed), reopening")
			w.markSerialDead()
			return true
		}
	}

	// Сначала применяем переходы AUTO <-> MANUAL, чтобы перед фазой
	// команд worker уже знал актуальный режим каждого AC.
	w.syncControlModes()

	// Фаза 1: приоритетный обход pending-команд.
	for _, dev := range w.cfg.Devices {
		if !isRunning() {
			break
		}
		addr := int(dev.Address)
		if !w.initialized[addr] {
			continue
		}
		if w.hasPendingCommand(addr) {
			w.checkAndSendCommand(addr)
		}
	}

	// Фаза 2: полный опрос.
	for _, dev := range w.cfg.Devices {
		if !isRunning() {
			break
		}
		if w.ser == nil {
			break
		}
		addr := int(dev.Address)
		w.pollOne(addr)
	}

	return true
}

func (w *worker) run() {
	backoff := serialRetryDelay
	lastFailTime := time.Time{}

	for isRunning() {
		if w.ser == nil {
			// Сброс backoff, если с последней неудачи прошло много времени.
			if !lastFailTime.IsZero() && time.Since(lastFailTime) > serialMaxBackoff {
				backoff = serialRetryDelay
			}

			if !w.openSerial() {
				lastFailTime = time.Now()
				time.Sleep(backoff)
				if backoff < serialMaxBackoff {
					backoff *= 2
					if backoff > serialMaxBackoff {
						backoff = serialMaxBackoff
					}
				}
				continue
			}
			backoff = serialRetryDelay
			lastFailTime = time.Time{}
			w.initialized = map[int]bool{}
		}

		if !w.cycle() {
			w.markSerialDead()
			time.Sleep(workerPanicDelay)
		}
	}
	w.closeSerial()
}

// ==========================================
// 4. MAIN
// ==========================================

func main() {
	exe, err := os.Executable()
	dir := "."
	if err == nil {
		dir = filepath.Dir(exe)
	}

	if rw, err := newRotWriter(filepath.Join(dir, "mdv_gateway.log"), 512*1024, 2); err == nil {
		logW = rw
	}

	cfgPath := filepath.Join(dir, "config.json")
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigCh
		logf(lvlInfo, "Received signal %v, shutting down...", s)
		atomic.StoreInt32(&running, 0)
		time.Sleep(200 * time.Millisecond)
		os.Exit(0)
	}()

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		logf(lvlError, "Failed to load config: %v", err)
		fmt.Fprintf(os.Stderr, "Failed to load config %s: %v\n", cfgPath, err)
		os.Exit(1)
	}

	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		setLogLevel(lvlDebug)
	case "info":
		setLogLevel(lvlInfo)
	case "error":
		setLogLevel(lvlError)
	default:
		setLogLevel(lvlWarn)
	}

	logf(lvlInfo, "Starting MDV Gateway...")

	registers = make([]int, len(cfg.Devices)*regsPerDevice)
	logf(lvlInfo, "Initialized %d devices, %d registers", len(cfg.Devices), len(registers))

	go startModbusServer(cfg.Modbus.Host, int(cfg.Modbus.Port))

	newWorker(cfg).run()

	logf(lvlInfo, "Gateway stopped")
}
