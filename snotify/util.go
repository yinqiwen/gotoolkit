package main

import (
	"time"
)

var (
	// 具体的方法是:用一位来表示一个月的大小，大月记为1，小月记为0，
	// 这样就用掉12位(无闰月)或13位(有闰月)，再用高四位来表示闰月的月份，没有闰月记为0。
	// 例如：2000年的信息数据是0xc96，化成二进制就是110010010110B，表示的
	// 含义是:1、2、5、8、10、11月大，其余月份小。
	// Since 1900~2050
	lunarTable = []int{
		0x04bd8, 0x04ae0, 0x0a570, 0x054d5, 0x0d260,
		0x0d950, 0x16554, 0x056a0, 0x09ad0, 0x055d2,
		0x04ae0, 0x0a5b6, 0x0a4d0, 0x0d250, 0x1d255,
		0x0b540, 0x0d6a0, 0x0ada2, 0x095b0, 0x14977,
		0x04970, 0x0a4b0, 0x0b4b5, 0x06a50, 0x06d40,
		0x1ab54, 0x02b60, 0x09570, 0x052f2, 0x04970,
		0x06566, 0x0d4a0, 0x0ea50, 0x06e95, 0x05ad0,
		0x02b60, 0x186e3, 0x092e0, 0x1c8d7, 0x0c950,
		0x0d4a0, 0x1d8a6, 0x0b550, 0x056a0, 0x1a5b4,
		0x025d0, 0x092d0, 0x0d2b2, 0x0a950, 0x0b557,
		0x06ca0, 0x0b550, 0x15355, 0x04da0, 0x0a5d0,
		0x14573, 0x052d0, 0x0a9a8, 0x0e950, 0x06aa0,
		0x0aea6, 0x0ab50, 0x04b60, 0x0aae4, 0x0a570,
		0x05260, 0x0f263, 0x0d950, 0x05b57, 0x056a0,
		0x096d0, 0x04dd5, 0x04ad0, 0x0a4d0, 0x0d4d4,
		0x0d250, 0x0d558, 0x0b540, 0x0b5a0, 0x195a6,
		0x095b0, 0x049b0, 0x0a974, 0x0a4b0, 0x0b27a,
		0x06a50, 0x06d40, 0x0af46, 0x0ab60, 0x09570,
		0x04af5, 0x04970, 0x064b0, 0x074a3, 0x0ea50,
		0x06b58, 0x055c0, 0x0ab60, 0x096d5, 0x092e0,
		0x0c960, 0x0d954, 0x0d4a0, 0x0da50, 0x07552,
		0x056a0, 0x0abb7, 0x025d0, 0x092d0, 0x0cab5,
		0x0a950, 0x0b4a0, 0x0baa4, 0x0ad50, 0x055d9,
		0x04ba0, 0x0a5b0, 0x15176, 0x052b0, 0x0a930,
		0x07954, 0x06aa0, 0x0ad50, 0x05b52, 0x04b60,
		0x0a6e6, 0x0a4e0, 0x0d260, 0x0ea65, 0x0d530,
		0x05aa0, 0x076a3, 0x096d0, 0x04bd7, 0x04ad0,
		0x0a4d0, 0x1d0b6, 0x0d250, 0x0d520, 0x0dd45,
		0x0b5a0, 0x056d0, 0x055b2, 0x049b0, 0x0a577,
		0x0a4b0, 0x0aa50, 0x1b255, 0x06d20, 0x0ada0,
	}
	MinYear = 1900
)

//which month leaps in this year?
//return 1-12(if there is one) or 0(no leap month).
func leapMonth(year int) int {
	return int(lunarTable[year-MinYear] & 0xf)
}

//the days of this year's leap month
func leapDays(year int) int {
	if leapMonth(year) != 0 {
		if (lunarTable[year-MinYear] & 0x10000) != 0 {
			return 30
		}
		return 29
	}
	return 0
}

//the total days of this year
func yearDays(year int) int {
	sum := 348
	for i := 0x8000; i > 0x8; i >>= 1 {
		if (lunarTable[year-MinYear] & i) != 0 {
			sum++
		}
	}
	return sum + leapDays(year)
}

//the days of the m-th month of this year
func monthDays(year, month int) int {
	if (lunarTable[year-MinYear] & (0x10000 >> uint(month))) != 0 {
		return 30
	}
	return 29
}

func lunarTime(s time.Time) (int, int) {
	base := time.Date(1900, 1, 31, 0, 0, 0, 0, time.UTC)
	var i int
	var leap int
	var isLeap bool
	var temp int

	var day int
	var month int
	var year int

	//offset days
	offset := int(s.Sub(base).Seconds() / 86400)

	minYear := 1900
	maxYear := 2050
	for i = minYear; i < maxYear && offset > 0; i++ {
		temp = yearDays(i)
		offset -= temp
	}

	if offset < 0 {
		offset += temp
		i--
	}

	year = i

	leap = leapMonth(i)
	isLeap = false

	for i = 1; i < 13 && offset > 0; i++ {
		//leap month
		if leap > 0 && i == (leap+1) && isLeap == false {
			i--
			isLeap = true
			temp = leapDays(year)
		} else {
			temp = monthDays(year, i)
		}
		//reset leap month
		if isLeap == true && i == (leap+1) {
			isLeap = false
		}
		offset -= temp
	}

	if offset == 0 && leap > 0 && i == (leap+1) {
		if isLeap {
			isLeap = false
		} else {
			isLeap = true
			i--
		}
	}

	if offset < 0 {
		offset += temp
		i--
	}
	month = i
	day = offset + 1
	return month, day
}

func isNonTradeDayTime(ts time.Time) bool {
	if ts.Hour() < 9 || ts.Hour() >= 16 {
		return true
	}
	if ts.Weekday() == 0 || ts.Weekday() == 6 {
		return true
	}
	if ts.Month() == 10 {
		if ts.Day() <= 7 {
			return true
		}
	}

	lunarMonth, lunarDay := lunarTime(ts)
	if lunarMonth == 12 && lunarDay == 30 {
		return true
	}
	if lunarMonth == 1 && lunarDay < 7 {
		return true
	}

	return false
}
