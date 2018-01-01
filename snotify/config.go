package main

type PortfolioConfig struct {
	Name   string
	Weight int
}

type MailConfig struct {
	User   string
	Passwd string
	Notify []string
}

type SnotifyConfig struct {
	Log          []string
	Mail         MailConfig
	RedisAddr    string
	UA           string
	User         string
	Passwd       string
	DataDir      string
	Portfolio    []PortfolioConfig
	ScanPeriod   int
	ScanInterval int
}

type RebalanceEntry struct {
	Id                 int     `json:"id"`
	RebalancingId      int     `json:"rebalancing_id"`
	StockId            int     `json:"stock_id"`
	StockName          string  `json:"stock_name"`
	StockSymbol        string  `json:"stock_symbol"`
	Volume             float32 `json:"volume"`
	PrevVolume         float32 `json:"prev_volume"`
	TargetVolume       float32 `json:"target_volume"`
	PrevTargetVolume   float32 `json:"prev_target_volume"`
	Price              float32 `json:"price"`
	PrevPrice          float32 `json:"prev_price"`
	NetValue           float32 `json:"net_value"`
	PrevNetValue       float32 `json:"prev_net_value"`
	Weight             float32 `json:"weight"`
	TargetWeight       float32 `json:"target_weight"`
	PrevWeight         float32 `json:"prev_weight"`
	PrevTargetWeight   float32 `json:"prev_target_weight"`
	PrevWeightAdjusted float32 `json:"prev_weight_adjusted"`
	Proactive          bool    `json:"proactive"`
	CreatedAt          int64   `json:"created_at"`
	UpdatedAt          int64   `json:"updated_at"`
}

type RebalanceItem struct {
	Id                int              `json:"id"`
	Status            string           `json:"status"`
	CubeId            int              `json:"cube_id"`
	PrevRebalancingId int              `json:"prev_bebalancing_id"`
	Category          string           `json:"category"`
	ExeStrategy       string           `json:"exe_strategy"`
	CreatedAt         int64            `json:"created_at"`
	UpdatedAt         int64            `json:"updated_at"`
	CashValue         float32          `json:"cash_value"`
	Cash              float32          `json:"cash"`
	ErrorCode         string           `json:"error_code"`
	ErrorMessage      string           `json:"error_message"`
	ErrorStatus       string           `json:"error_status"`
	Holdings          string           `json:"holdings"`
	Entry             []RebalanceEntry `json:"rebalancing_histories"`
}

type RebalanceRecord struct {
	Count      int             `json:"count"`
	Page       int             `json:"page"`
	TotalCount int             `json:"totalCount"`
	Items      []RebalanceItem `json:"list"`
}
