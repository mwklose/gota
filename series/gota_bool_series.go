package series

type GotaBoolSeries struct {
	Name     string       // The name of the series
	elements BoolElements // The values of the elements
	Err      error
}
