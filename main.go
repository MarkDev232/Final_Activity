package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func connectdb() *sql.DB {
	connstring := "host = localhost port=5432 user=postgres password=root dbname=tododb sslmode=disable "
	db, err := sql.Open("postgres", connstring)
	if err != nil {
		fmt.Println(err)
	}
	return db
}
func getid(db *sql.DB, rollid string) bool {
	var name, course string

	query := "SELECT name, course FROM student WHERE id = $1"
	err := db.QueryRow(query, rollid).Scan(&name, &course)
	if err != nil {
		if err == sql.ErrNoRows {
			return false // Student ID does not exist
		}
		log.Println("Error querying database:", err) // Log unexpected errors
		return false
	}

	return true // Student record exists
}

// Generate an 8-character UUID
func generateShortUUID() string {
	u := uuid.New().String() // Generate a full UUID
	return u[:8]             // Take the first 8 characters
}

func getinput() string {
	var data string
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		data = scanner.Text()
	}
	return data
}

func insertIntoDB(db *sql.DB, name string, course string, start_date string) {
	// Generate an 8-character ID
	shortID := uuid.New().String()[:8] // Get the first 8 characters

	// Insert student record
	_, err := db.Exec("INSERT INTO student (id, name, course, start_date, iscommpleted) VALUES ($1, $2, $3, $4, $5)",
		shortID, name, course, start_date, false)

	if err != nil {
		log.Println("Error inserting data:", err)
	} else {
		fmt.Println("Record Inserted with ID:", shortID)

		// Log the insert action
		_, err := db.Exec("INSERT INTO student_log (action_type, table_name, record_id, new_data) VALUES ($1, $2, $3, $4)",
			"INSERT", "student", shortID, fmt.Sprintf(`{"name": "%s", "course": "%s", "start_date": "%s", "iscommpleted": false}`, name, course, start_date))

		if err != nil {
			log.Println("Error logging insert:", err)
		}
	}
}

func ReadData(db *sql.DB) {
    fmt.Println("Read data from Postgres")

    // Ask user if they want filtering
    fmt.Println("Do you want to filter by:")
    fmt.Println("1. No Filter (Show All)")
    fmt.Println("2. Filter by ID")
    fmt.Println("3. Filter by Name")
    fmt.Println("4. Filter by Course")
    fmt.Print("Enter choice (1-4): ")

    filterChoice := getinput()
    var filterQuery string
    var filterValue string

    switch filterChoice {
    case "2":
        fmt.Print("Enter ID to filter: ")
        filterValue = getinput()
        filterQuery = " AND id = $1"
    case "3":
        fmt.Print("Enter Name to filter: ")
        filterValue = getinput()
        filterQuery = " AND name ILIKE $1"
        filterValue = "%" + filterValue + "%"
    case "4":
        fmt.Print("Enter Course to filter: ")
        filterValue = getinput()
        filterQuery = " AND course ILIKE $1"
        filterValue = "%" + filterValue + "%"
    default:
        filterQuery = "" // No filter
    }

    // Ask user for sorting order
    fmt.Println("Sort by:")
    fmt.Println("1. Name ASC")
    fmt.Println("2. Name DESC")
    fmt.Println("3. Course ASC")
    fmt.Println("4. Course DESC")
    fmt.Print("Enter choice (1-4): ")
    sortChoice := getinput()

    var sortQuery string
    switch sortChoice {
    case "1":
        sortQuery = " ORDER BY name ASC"
    case "2":
        sortQuery = " ORDER BY name DESC"
    case "3":
        sortQuery = " ORDER BY course ASC"
    case "4":
        sortQuery = " ORDER BY course DESC"
    default:
        sortQuery = " ORDER BY id ASC" // Default sorting
    }

    // Ask for pagination
    fmt.Print("Enter number of records per page: ")
    var recordsPerPage int
    fmt.Scanln(&recordsPerPage)

    if recordsPerPage <= 0 {
        recordsPerPage = 10 // Default to 10 per page if invalid input
    }

    // Get total record count
    countQuery := "SELECT COUNT(*) FROM student WHERE isdeleted = false" + filterQuery
    var totalRecords int
    var err error
    
    if filterQuery != "" {
        err = db.QueryRow(countQuery, filterValue).Scan(&totalRecords)
    } else {
        err = db.QueryRow(countQuery).Scan(&totalRecords)
    }
    
    if err != nil {
        log.Println("Error fetching total records:", err)
        return
    }

    totalPages := (totalRecords + recordsPerPage - 1) / recordsPerPage // Ceiling division
    fmt.Printf("Total records: %d, Total pages: %d\n", totalRecords, totalPages)

    // Loop through pages automatically
    for page := 1; page <= totalPages; page++ {
        offset := (page - 1) * recordsPerPage
        query := "SELECT id, name, course, iscommpleted FROM student WHERE isdeleted = false" + filterQuery + sortQuery + " LIMIT $1 OFFSET $2"

        var rows *sql.Rows
        if filterQuery != "" {
            rows, err = db.Query(query, filterValue, recordsPerPage, offset)
        } else {
            rows, err = db.Query(query, recordsPerPage, offset)
        }

        if err != nil {
            log.Println("Error executing query:", err)
            return
        }
        defer rows.Close()

        fmt.Printf("\nPage %d/%d:\n", page, totalPages)
        fmt.Printf("%-36s %-20s %-20s %-20s\n", "ID", "Name", "Course", "IsCompleted")
        fmt.Println(strings.Repeat("-", 100))

        var id, name, course string
        var isCompleted bool

        for rows.Next() {
            err := rows.Scan(&id, &name, &course, &isCompleted)
            if err != nil {
                log.Println("Error scanning row:", err)
                return
            }
            fmt.Printf("%-36s %-20s %-20s %-20t\n", id, name, course, isCompleted)
        }

        if err = rows.Err(); err != nil {
            log.Println("Error iterating rows:", err)
        }
    }
}



func viewLogs(db *sql.DB) {
	rows, err := db.Query("SELECT log_id, action_type, table_name, record_id, old_data, new_data, changed_at FROM student_log ORDER BY changed_at DESC")
	if err != nil {
		log.Println("Error fetching logs:", err)
		return
	}
	defer rows.Close()

	fmt.Printf("%-5s %-10s %-10s %-10s %-30s %-30s %-20s\n", "ID", "Action", "Table", "Record ID", "Old Data", "New Data", "Timestamp")
	fmt.Println(strings.Repeat("-", 120))

	var id, actionType, tableName, recordID, oldData, newData, changedAt sql.NullString // Use sql.NullString to handle NULL values

	for rows.Next() {
		err := rows.Scan(&id, &actionType, &tableName, &recordID, &oldData, &newData, &changedAt)
		if err != nil {
			log.Println("Error scanning log row:", err)
			continue
		}

		// Convert NULL values to empty strings for better output handling
		fmt.Printf("%-5s %-10s %-10s %-10s %-30s %-30s %-20s\n",
			nullToString(id),
			nullToString(actionType),
			nullToString(tableName),
			nullToString(recordID),
			nullToString(oldData),
			nullToString(newData),
			nullToString(changedAt),
		)
	}

	if err = rows.Err(); err != nil {
		log.Println("Error iterating log rows:", err)
	}
}

// Helper function to handle NULL values
func nullToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return "NULL"
}



// Deleteall deletes a student record and logs the deletion in the logs table.
func Deleteall(db *sql.DB, id string) {
	// Validate input ID (ensure it's not empty)
	if id == "" {
		log.Println("Invalid ID: cannot be empty")
		return
	}

	// Get the old data before deletion
	var oldName, oldCourse string
	var oldStatus bool
	err := db.QueryRow("SELECT name, course, iscommpleted FROM student WHERE id = $1", id).
		Scan(&oldName, &oldCourse, &oldStatus)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Println("Error: Student not found")
		} else {
			log.Println("Error fetching student data:", err)
		}
		return
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		log.Println("Error starting transaction:", err)
		return
	}

	// Perform soft delete
	_, err = tx.Exec("UPDATE student SET isdeleted = TRUE WHERE id = $1", id)
	if err != nil {
		log.Println("Error updating student record (soft delete):", err)
		tx.Rollback()
		return
	}

	// Log the delete action securely
	_, err = tx.Exec(`
		INSERT INTO student_log (action_type, table_name, record_id, old_data, new_data) 
		VALUES ('DELETE', 'student', $1, 
		jsonb_build_object('name', $2::text, 'course', $3::text, 'iscommpleted', $4::boolean), 
		'{}'::jsonb)`, id, oldName, oldCourse, oldStatus)

	if err != nil {
		log.Println("Error logging deletion:", err)
		tx.Rollback()
		return
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Println("Error committing transaction:", err)
		return
	}

	fmt.Println("Record marked as deleted for ID:", id)
}


// func readall()  {

// }
func main() {
	db := connectdb()

	var name string
	var course string

	var choice string
	for {
		fmt.Println("Enter you choice")
		fmt.Println("1. Insert data in Postgres")
		fmt.Println("2. Read data from Postgres")
		fmt.Println("3. Update data from Postgres")
		fmt.Println("4. Delete data from Postgres")
		fmt.Println("5. View Logs")
		fmt.Println("6. Exit")
		fmt.Scanln(&choice)
		switch choice {
		case "1":
			fmt.Println("Insert data in Postgres")

			fmt.Println("Enter Name: ")
			name = getinput()
			fmt.Println("Enter Course: ")
			course = getinput()

			// Get current date as a string (format: YYYY-MM-DD)
			currentDate := time.Now().Format("2006-01-02")
			insertIntoDB(db, name, course, currentDate)
			//insert data in Postgres

		case "2":
			ReadData(db)

		case "3":
			fmt.Println("Update data from Postgres")
			// Prompt user for ID
			fmt.Print("Enter ID to update: ")
			id := getinput()

			// Validate if the ID exists
			if !getid(db, id) {
				fmt.Printf("No record found for student ID %s.\n", id)
				return
			}

			fmt.Println("Record found! What do you want to do?")
			fmt.Println("1. Mark as Completed")
			fmt.Println("2. Update Name & Course")
			fmt.Print("Enter choice: ")

			choice := getinput()

			switch choice {
			case "1":
				// Mark student as completed
				tx, err := db.Begin()
				if err != nil {
					fmt.Println("Error starting transaction:", err)
					return
				}

				// Generate an 8-character log_id
				logID := generateShortUUID()

				_, err = tx.Exec("UPDATE student SET iscommpleted = TRUE WHERE id = $1", id)
				if err != nil {
					fmt.Println("Error updating record:", err)
					tx.Rollback()
					return
				}

				// Log the action securely
				_, err = tx.Exec(`
				INSERT INTO student_log (log_id, action_type, record_id, old_data, new_data) 
				VALUES ($1, 'UPDATE', $2, 
						(SELECT jsonb_build_object('name', name, 'course', course, 'iscommpleted', iscommpleted) 
						 FROM student WHERE id = $2), 
						'{"iscommpleted": true}'::jsonb)`, logID, id)

				if err != nil {
					fmt.Println("Error logging update:", err)
					tx.Rollback()
					return
				}

				// Commit transaction
				err = tx.Commit()
				if err != nil {
					fmt.Println("Error committing transaction:", err)
					return
				}

				fmt.Println("Student marked as completed.")

			case "2":
				fmt.Print("Enter New Name: ")
				name := getinput()
				fmt.Print("Enter New Course: ")
				course := getinput()

				tx, err := db.Begin()
				if err != nil {
					fmt.Println("Error starting transaction:", err)
					return
				}

				// Generate an 8-character log_id
				logID := generateShortUUID()

				_, err = tx.Exec("UPDATE student SET name = $1, course = $2 WHERE id = $3", name, course, id)
				if err != nil {
					fmt.Println("Error updating record:", err)
					tx.Rollback()
					return
				}

				// Log the update securely
				_, err = tx.Exec(`
				INSERT INTO student_log (log_id, action_type, record_id, old_data, new_data) 
				VALUES ($1, 'UPDATE', $2, 
						(SELECT jsonb_build_object('name', name, 'course', course, 'iscommpleted', iscommpleted) 
						 FROM student WHERE id = $2), 
						jsonb_build_object('name', $3::TEXT, 'course', $4::TEXT))`, logID, id, name, course)

				if err != nil {
					fmt.Println("Error logging update:", err)
					tx.Rollback()
					return
				}

				// Commit transaction
				err = tx.Commit()
				if err != nil {
					fmt.Println("Error committing transaction:", err)
					return
				}

				fmt.Println("Student record updated successfully.")

			default:
				fmt.Println("Invalid choice. Update canceled.")
			}

		case "4":
			fmt.Println("Delete data from Postgres")
			fmt.Println("Enter ID want to delete")
			id := getinput()
			Deleteall(db, id)
		case "5":
			fmt.Println("View Logs")
			viewLogs(db)
		case "6":
			fmt.Println("Exiting.........")
			
			os.Exit(0)
		default:
			
			fmt.Println("Invalid choice")
		}

	}
}
