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
	countrySet := make(map[string]string)
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
				start := ip2int(startIP)
				end := start + uint32(1<<(32-uint32(ones))) - 1
				c.ranges = append(c.ranges, ipRange{start, end, country})
				//log.Printf("####%d-%d %s", start, end, line)
			} else {
				sp := strings.Split(line, "|")
				if len(sp) >= 6 {
					var ipCountry string
					if len(country) > 0 && country != sp[1] {
						continue
					}
					if len(country) > 0 {
						ipCountry = country
					} else {
						if v, exist := countrySet[sp[1]]; !exist {
							countrySet[sp[1]] = sp[1]
							ipCountry = sp[1]
						} else {
							ipCountry = v
						}
					}

					if sp[2] == "ipv4" {
						startIP := net.ParseIP(sp[3])
						if nil != startIP {
							v := ip2int(startIP)
							ipcount, _ := strconv.ParseUint(sp[4], 10, 32)
							c.ranges = append(c.ranges, ipRange{v, v + uint32(ipcount-1), ipCountry})
							//log.Printf("####%d-%d %s", v, v+uint32(ipcount-1), line)
						}
					}
				}
			}
		}
	}
	sort.Sort(c)
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
		err = c.Load(f, country)
	}
	if nil != err {
		return nil, err
	}
	return c, nil
}
