package wal

/*
func TestOrderedLinkedList_Insert(t *testing.T) {
	oll := NewOrderedSegmentList()

	// Open files for demonstration purposes
	file1, _ := os.CreateTemp("", "1")
	file2, _ := os.CreateTemp("", "2")
	file3, _ := os.CreateTemp("", "3")
	file4, _ := os.CreateTemp("", "4")
	defer os.Remove(file1.Name())
	defer os.Remove(file2.Name())
	defer os.Remove(file3.Name())
	defer os.Remove(file4.Name())

	oll.Insert(5, file1)
	oll.Insert(2, file2)
	oll.Insert(7, file3)
	oll.Insert(3, file3)

	value := oll.Find(7)
	fmt.Println("Found:", value)

	value = oll.Find(4)
	fmt.Println("Found:", value)

	current := oll.Head
	for current != nil {
		fmt.Printf("Index: %d, Value: %v\n", current.Data.Index, current.Data.Value)
		current = current.Next
	}

	// Close files after use
	file1.Close()
	file2.Close()
	file3.Close()
}
*/
