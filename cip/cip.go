package cip

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

var ErrNoCountryFound = errors.New("No country found")

func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	}
	return binary.BigEndian.Uint32(ip)
}
func inet_ntoa(ipnr int64) net.IP {
	var bytes [4]byte
	bytes[0] = byte(ipnr & 0xFF)
	bytes[1] = byte((ipnr >> 8) & 0xFF)
	bytes[2] = byte((ipnr >> 16) & 0xFF)
	bytes[3] = byte((ipnr >> 24) & 0xFF)

	return net.IPv4(bytes[3], bytes[2], bytes[1], bytes[0])
}

func inet_aton(ipnr net.IP) int64 {
	bits := strings.Split(ipnr.String(), ".")

	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])

	var sum int64
	sum += int64(b0) << 24
	sum += int64(b1) << 16
	sum += int64(b2) << 8
	sum += int64(b3)
	return sum
}

type ipRange struct {
	Start, End uint32
	Country    string
}

type CountryIPSet struct {
	ranges []ipRange
}

func (c *CountryIPSet) Load(rc io.ReadCloser, country string) error {
	var err error
	reader := bufio.NewReader(rc)
	var buffer bytes.Buffer
	var (
		part   []byte
		prefix bool
	)
	defer rc.Close()
	for {
		if part, prefix, err = reader.ReadLine(); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		buffer.Write(part)
		if !prefix {
			line := buffer.String()
			buffer.Reset()
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.Contains(line, "/") {
				ip, ipnet, err := net.ParseCIDR(line)
				if nil != err {
					continue
				}
				startIP := ip
				ones, _ := ipnet.Mask.Size()
				v := inet_aton(ip)
				var tmp uint32
				tmp = 0xFFFFFFFF
				tmp = tmp >> uint32(ones)
				v = v | int64(tmp)
				endIP := inet_ntoa(v)
				c.ranges = append(c.ranges, ipRange{ip2int(startIP), ip2int(endIP), country})
			} else {
				sp := strings.Split(line, "|")
				if len(sp) >= 6 {
					if len(country) > 0 && country != sp[1] {
						continue
					}
					if sp[2] == "ipv4" {
						startIP := net.ParseIP(sp[3])
						if nil != startIP {
							v := ip2int(startIP)
							ipcount, _ := strconv.ParseUint(sp[4], 10, 32)
							c.ranges = append(c.ranges, ipRange{v, v + uint32(ipcount-1), sp[1]})
						}
					}
				}
			}
		}
	}
	sort.Sort(c)
	//fmt.Printf("###%v %v", c.ranges[0], c.ranges[1])
	return nil
}

func (c *CountryIPSet) Len() int {
	return len(c.ranges)
}

// Less returns whether the element with index i should sort
// before the element with index j.
func (c *CountryIPSet) Less(i, j int) bool {
	if c.ranges[i].Start < c.ranges[j].Start {
		return true
	}
	if c.ranges[i].Start == c.ranges[j].Start {
		return c.ranges[i].End < c.ranges[j].End
	}
	return false
}

// Swap swaps the elements with indexes i and j.
func (c *CountryIPSet) Swap(i, j int) {
	tmp := c.ranges[i]
	c.ranges[i] = c.ranges[j]
	c.ranges[j] = tmp
}

func (c *CountryIPSet) FindCountry(ip net.IP) (string, error) {
	if ip.To4() == nil {
		return "", fmt.Errorf("Only IPv4 supported")
	}
	v := ip2int(ip)

	compare := func(i int) bool {
		if i < 0 {
			return false
		}
		if i >= len(c.ranges) {
			return true
		}
		if c.ranges[i].Start > v {
			return true
		}
		if c.ranges[i].Start <= v && c.ranges[i].End >= v {
			return true
		}
		return false
	}
	index := sort.Search(len(c.ranges), compare)
	if index == len(c.ranges) {
		//log.Printf("####%d\n", len(h.ranges))
		return "", ErrNoCountryFound
	}
	if v >= c.ranges[index].Start && v <= c.ranges[index].End {
		return c.ranges[index].Country, nil
	}
	return "", ErrNoCountryFound
}

func (c *CountryIPSet) IsInCountry(ip net.IP, country string) bool {
	found, err := c.FindCountry(ip)
	if nil == err {
		return country == found
	}
	return false
}

func LoadApnicIPSet(file string, country string) (*CountryIPSet, error) {
	return LoadIPSet(file, country)
}
func LoadIPSet(file string, country string) (*CountryIPSet, error) {
	c := &CountryIPSet{}
	f, err := os.Open(file)
	if nil == err {
		err = c.Load(f, "")
	}
	if nil != err {
		return nil, err
	}
	return c, nil
}
