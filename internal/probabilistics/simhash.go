package probabilistics

import (
	"crypto/md5"
	"regexp"
	"strings"
)

var commonWords = map[string]bool{
	"the": true, "a": true, "an": true, "in": true, "and": true, "or": true,
	"i": true, "you": true, "he": true, "she": true, "it": true, "we": true,
	"they": true, "me": true, "him": true, "her": true, "us": true, "this": true,
	"that": true, "these": true, "those": true, "there": true, "here": true,
	"where": true, "what": true, "when": true, "while": true, "which": true,
	"who": true, "how": true, "if": true, "yes": true, "no": true, "not": true,
	"be": true, "been": true, "being": true, "is": true, "was": true, "were": true,
	"am": true, "are": true, "have": true, "has": true, "do": true, "does": true,
}

func tokenize(text string) []string {
	re := regexp.MustCompile(`\w+`)
	words := re.FindAllString(strings.ToLower(text), -1)

	var tokens []string
	for _, word := range words {
		if !commonWords[word] {
			tokens = append(tokens, word)
		}
	}
	return tokens
}

func wordHash(word string) [128]int {
	hash := md5.Sum([]byte(word))
	var bits [128]int

	for i := 0; i < len(hash); i++ {
		for j := 0; j < 8; j++ {
			if hash[i]&(1<<uint(j)) > 0 {
				bits[i*8+j] = 1
			} else {
				bits[i*8+j] = -1
			}
		}
	}
	return bits
}

func simhash(text string) uint64 {
	tokens := tokenize(text)
	var vector [128]int

	for _, token := range tokens {
		hashBits := wordHash(token)
		for i := 0; i < 128; i++ {
			vector[i] += hashBits[i]
		}
	}

	var hash uint64
	for i := 0; i < 64; i++ {
		if vector[i] > 0 {
			hash |= 1 << uint(i)
		}
	}
	return hash
}