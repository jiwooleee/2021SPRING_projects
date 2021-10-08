// DO NOT EDIT THIS PART
// Your task is to edit `main.go`

package main

import (
	"errors"
	"strings"
	"time"
)

// GetMockStream is a blackbox function which returns a mock stream for
// demonstration purposes
func GetMockStream() Stream {
	return Stream{0, mockdata}
}

// Stream is a mock stream for demonstration purposes, not threadsafe
type Stream struct {
	pos    int
	tweets []Tweet
}

// ErrEOF returns on End of File error
var ErrEOF = errors.New("End of File")

// Next returns the next Tweet in the stream, returns EOF error if
// there are no more tweets
func (s *Stream) Next() (*Tweet, error) {

	// simulate delay
	time.Sleep(320 * time.Millisecond)
	if s.pos >= len(s.tweets) {
		return &Tweet{}, ErrEOF
	}

	tweet := s.tweets[s.pos]
	s.pos++

	return &tweet, nil
}

// Tweet defines the simlified representation of a tweet
type Tweet struct {
	Username string
	Text     string
}

// IsTalkingAboutRhodes is a mock process which pretend to be a sophisticated
// procedure to analyse whether tweet is talking about Rhodes or not.
func (t *Tweet) IsTalkingAboutRhodes() bool {
	// simulate delay
	time.Sleep(330 * time.Millisecond)

	hasRhodes := strings.Contains(strings.ToLower(t.Text), "rhodes")
	hasLynx := strings.Contains(strings.ToLower(t.Text), "lynx")

	return hasRhodes || hasLynx
}

var mockdata = []Tweet{
	{
		"PresidentHass",
		"Computer Science is the best program at #Rhodes!",
	}, {
		"realsewanee",
		"Our computer science program is okay...but not great.",
	}, {
		"PJOfficial",
		"I'm not a cat I'm a Lynx",
	}, {
		"PJOfficial",
		"Feed me",
	}, {
		"realsewanee",
		"Rhodes sux",
	},
}
