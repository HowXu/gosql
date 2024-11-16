package err

type DatabaseError struct{
	Msg string
}

func (e *DatabaseError) Error() string{
	return e.Msg
}