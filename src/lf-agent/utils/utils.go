package utils

type TaskFunc func() error

func RunSequentially(tasks ...TaskFunc) error {
	for _, task := range tasks {
		if err := task(); err != nil {
			return err
		}
	}
	return nil
}
