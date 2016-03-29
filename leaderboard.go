package leaderboard

import (
	"fmt"
	"math"
	"net"

	"github.com/garyburd/redigo/redis"
	"appengine/socket"
)

/* Structs model */
type User struct {
	PlayerID 	uint64
	Score		int
	Rank 		int
}

type Team struct {
	Name    string
	Members map[string]User
	Rank    int
}

type RedisSettings struct {
	Host     string
	Password string
	// Added context so we have the currently valid AppEngine context.
	Context appengine.Context
}

type Leaderboard struct {
	AppEngine bool
	Settings RedisSettings
	Name     string
	PageSize int
}

var pool *redis.Pool

/* Private functions */

func newPool(server string, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func getConnection(settings RedisSettings) redis.Conn {

	if settings.AppEngine {
		// Since Google App Engine requires the use of socket.Dial (from appengine/socket), we have to override the default dialer.
		// To add the custom dialer, we need to use redis.DialNetDial.
		// redis.DialNetDial allows us to specify a custom dialer through the function signature func(string, string) (net.Conn, error).
		c, err := redis.Dial("tcp", settings.Host, redis.DialNetDial(func(network, addr string) (net.Conn, error) {
			return socket.Dial(settings.Context, network, addr) }))

		if err != nil {
			fmt.Printf("Redis connection error: %v", err)
			return nil
		}

		if settings.Password != "" {
			if _, err := c.Do("AUTH", settings.Password); err != nil {
				c.Close()
				fmt.Printf("Redis connection error: %v", err)
				return nil
			}
		}

		return c
	} else if pool == nil {
			pool = newPool(settings.Host, settings.Password)
			return pool.Get()
	}
}

func getMembersByRange(settings RedisSettings, leaderboard string, pageSize int, startOffset int, endOffset int) []User {
	conn := getConnection(settings)
	defer conn.Close()
	users := make([]User, pageSize)
	values, _ := redis.Values(conn.Do("ZREVRANGE", leaderboard, startOffset, endOffset, "WITHSCORES"))
	var i = 0
	for len(values) > 0 {
		playerid := uint64(0)
		score := -1
		values, _ = redis.Scan(values, &playerid, &score)
		rank, _ := redis.Int(conn.Do("ZREVRANK", leaderboard, playerid))
		nUser := User{
			PlayerID: playerid,
			Score: score,
			Rank: rank + 1,
		}
		users[i] = nUser
		i += 1
	}
	return users
}

/* End Private functions */

/* Public functions */

func NewLeaderboard(settings RedisSettings, name string, pageSize int) Leaderboard {
	l := Leaderboard{settings: settings, name: name, pageSize: pageSize}
	return l
}

func (l *Leaderboard) RankMember(playerID uint64, score int) (User, error) {
	conn := getConnection(l.Settings)
	defer conn.Close()
	_, err := conn.Do("ZADD", l.Name, score, playerID)
	if err != nil {
		fmt.Printf("error on store in redis in rankMember Leaderboard:%s - Username:%s - Score:%d", l.Name, playerID, score)
	}
	rank, err := redis.Int(conn.Do("ZREVRANK", l.Name, playerID))
	if err != nil {
		fmt.Printf("error on get user rank Leaderboard:%s - Username:%s", l.Name, playerID)
		rank = -1
	}
	nUser := User{
		PlayerID: 	playerID,
		Score: 		score,
		Rank: 		rank + 1,
	}
	return nUser, err
}

func (l *Leaderboard) TotalMembers() int {
	conn := getConnection(l.Settings)
	defer conn.Close()
	total, err := redis.Int(conn.Do("ZCARD", l.Name))
	if err != nil {
		fmt.Printf("error on get leaderboard total members")
		return 0
	}
	return total
}

func (l *Leaderboard) RemoveMember(playerID uint64) (User, error) {
	conn := getConnection(l.Settings)
	defer conn.Close()
	nUser := l.GetMember(playerID)
	_, err := conn.Do("ZREM", l.Name, playerID)
	if err != nil {
		fmt.Printf("error on remove user from leaderboard")
	}
	return nUser, err
}

func (l *Leaderboard) TotalPages() int {
	conn := getConnection(l.Settings)
	defer conn.Close()
	pages := 0
	total, err := redis.Int(conn.Do("ZCOUNT", l.Name, "-inf", "+inf"))
	if err == nil {
		pages = int(math.Ceil(float64(total) / float64(l.PageSize)))
	}
	return pages
}

func (l *Leaderboard) GetMember(playerID uint64) User {
	conn := getConnection(l.Settings)
	defer conn.Close()
	rank, err := redis.Int(conn.Do("ZREVRANK", l.Name, playerID))
	if err != nil {
		rank = 0
	}
	score, err := redis.Int(conn.Do("ZSCORE", l.Name, playerID))
	if err != nil {
		score = 0
	}
	nUser := User{
		PlayerID: playerID,
		Score: score,
		Rank: rank + 1,
	}
	// If err is not nil, it will pose a problem for error checking
	// since it was eaten above, therefore err was removed from being passed back.
	return nUser
}

func (l *Leaderboard) GetAroundMe(playerID uint64) []User {
	currentUser := l.GetMember(playerID)
	startOffset := currentUser.Rank - (l.PageSize / 2)
	if startOffset < 0 {
		startOffset = 0
	}
	endOffset := (startOffset + l.PageSize) - 1
	return getMembersByRange(l.Settings, l.Name, l.PageSize, startOffset, endOffset)
}

func (l *Leaderboard) GetRank(playerID uint64) int {
	conn := getConnection(l.Settings)
	rank, _ := redis.Int(conn.Do("ZREVRANK", l.Name, playerID))
	defer conn.Close()
	return rank + 1
}

func (l *Leaderboard) GetLeaders(page int) []User {
	if page < 1 {
		page = 1
	}
	if page > l.TotalPages() {
		page = l.TotalPages()
	}
	redisIndex := page - 1
	startOffset := redisIndex * l.PageSize
	if startOffset < 0 {
		startOffset = 0
	}
	endOffset := (startOffset + l.PageSize) - 1

	return getMembersByRange(l.Settings, l.Name, l.PageSize, startOffset, endOffset)
}

func (l *Leaderboard) GetMemberByRank(position int) User {
	conn := getConnection(l.Settings)

	if position <= l.TotalMembers() {
		currentPage := int(math.Ceil(float64(position) / float64(l.PageSize)))
		offset := (position - 1) % l.PageSize
		leaders := l.GetLeaders(currentPage)
		defer conn.Close()
		if leaders[offset].Rank == position {
			return leaders[offset]
		}
	}
	defer conn.Close()
	return User{}
}

// Clears out all the databases
func (l *Leaderboard) FlushDB() (err error) {
	conn := getConnection(l.Settings)
	defer conn.Close()

	_, err = conn.Do("FLUSHALL")
	if err != nil {
		fmt.Printf("error on remove user from leaderboard")
	}
	return err
}

/* End Public functions */
