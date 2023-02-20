package avro

// LogicalDuration represents the `duration` logical type, as defined in
// https://avro.apache.org/docs/1.11.1/specification/#duration
type LogicalDuration struct {
	Months       uint32
	Days         uint32
	Milliseconds uint32
}
