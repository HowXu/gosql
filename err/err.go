package err

import (
	"strconv"
)

type DatabaseError struct {
	Msg string
}

func (e *DatabaseError) Error() string {
	return e.Msg
}

type CommandError struct {
	Trap bool
}

func (e *CommandError) Error() string {
	return "trap to user command line: " + strconv.FormatBool(e.Trap)
}

type SyntaxError struct {
	Msg string
}

func (e *SyntaxError) Error() string {
	return "syntax error: " + e.Msg
}

type PermissionError struct {
	Msg string
}

func (e *PermissionError) Error() string {
	return "permission error: " + e.Msg
}

type CryptError struct {
	Msg string
}

func (e *CryptError) Error() string {
	return "crypt error: " + e.Msg
}