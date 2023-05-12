package util

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateRandomConsumerName(prefix string) string {
	rand.Seed(time.Now().UnixNano())
	// Generate three random uppercase letters
	b := make([]byte, 3)
	for i := range b {
		b[i] = byte(rand.Intn(26) + 'A')
	}
	randomLetters := string(b)
	// Generate three random digits
	randomDigits := fmt.Sprintf("%05d", rand.Intn(1000))
	// Generate the date as yyyy-MM-dd-hhmm
	date := time.Now().Format("2006-01-02-1504")
	// Concatenate the parts to form the final random string
	randomString := fmt.Sprintf("%s-%s-%s", randomLetters, randomDigits, date)
	return prefix + "-" + randomString
}
