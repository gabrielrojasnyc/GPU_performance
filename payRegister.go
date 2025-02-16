package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Data structures for the three input files

type PayrollRecord struct {
	EmployeeID   string
	EmployeeName string
	JobTitle     string
	PayPeriod    string
	HourlyRate   float64
}

type TimeRecord struct {
	EmployeeID    string
	PayPeriod     string
	RegularHours  int
	OvertimeHours int
}

type BenefitsRecord struct {
	EmployeeID      string
	PayPeriod       string
	HealthInsurance float64
	Retirement      float64
	OtherBenefits   float64
}

// Structure for the computed pay register

type PayRegister struct {
	EmployeeID      string
	EmployeeName    string
	JobTitle        string
	PayPeriod       string
	HourlyRate      float64
	RegularHours    int
	OvertimeHours   int
	GrossWages      float64
	FederalTax      float64
	StateTax        float64
	SocialSecurity  float64
	Medicare        float64
	HealthInsurance float64
	Retirement      float64
	OtherBenefits   float64
	TotalBenefits   float64
	TotalDeductions float64
	NetPay          float64
}

// makeKey combines EmployeeID and PayPeriod for map keys.
func makeKey(employeeID, payPeriod string) string {
	return employeeID + "|" + payPeriod
}

// readPayrollRecords reads payroll_data.csv and returns a map keyed by EmployeeID|PayPeriod.
func readPayrollRecords(filename string) (map[string]PayrollRecord, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open payroll file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("cannot read payroll csv: %v", err)
	}

	payrollMap := make(map[string]PayrollRecord)
	// Skip header
	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) < 5 {
			continue
		}
		hourlyRate, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing Hourly Rate in row %d: %v", i+1, err)
		}
		rec := PayrollRecord{
			EmployeeID:   row[0],
			EmployeeName: row[1],
			JobTitle:     row[2],
			PayPeriod:    row[3],
			HourlyRate:   hourlyRate,
		}
		key := makeKey(rec.EmployeeID, rec.PayPeriod)
		payrollMap[key] = rec
	}
	return payrollMap, nil
}

// readTimeRecords reads time_data.csv and returns a map keyed by EmployeeID|PayPeriod.
func readTimeRecords(filename string) (map[string]TimeRecord, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open time file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("cannot read time csv: %v", err)
	}

	timeMap := make(map[string]TimeRecord)
	for i, row := range records {
		if i == 0 {
			continue // skip header
		}
		if len(row) < 4 {
			continue
		}
		regularHours, err := strconv.Atoi(row[2])
		if err != nil {
			return nil, fmt.Errorf("error parsing Regular Hours in row %d: %v", i+1, err)
		}
		overtimeHours, err := strconv.Atoi(row[3])
		if err != nil {
			return nil, fmt.Errorf("error parsing Overtime Hours in row %d: %v", i+1, err)
		}
		rec := TimeRecord{
			EmployeeID:    row[0],
			PayPeriod:     row[1],
			RegularHours:  regularHours,
			OvertimeHours: overtimeHours,
		}
		key := makeKey(rec.EmployeeID, rec.PayPeriod)
		timeMap[key] = rec
	}
	return timeMap, nil
}

// readBenefitsRecords reads benefits.csv and returns a map keyed by EmployeeID|PayPeriod.
func readBenefitsRecords(filename string) (map[string]BenefitsRecord, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open benefits file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("cannot read benefits csv: %v", err)
	}

	benefitsMap := make(map[string]BenefitsRecord)
	for i, row := range records {
		if i == 0 {
			continue
		}
		if len(row) < 5 {
			continue
		}
		healthInsurance, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing Health Insurance in row %d: %v", i+1, err)
		}
		retirement, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing Retirement in row %d: %v", i+1, err)
		}
		otherBenefits, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing Other Benefits in row %d: %v", i+1, err)
		}
		rec := BenefitsRecord{
			EmployeeID:      row[0],
			PayPeriod:       row[1],
			HealthInsurance: healthInsurance,
			Retirement:      retirement,
			OtherBenefits:   otherBenefits,
		}
		key := makeKey(rec.EmployeeID, rec.PayPeriod)
		benefitsMap[key] = rec
	}
	return benefitsMap, nil
}

// computeRegister computes the pay register by merging the three datasets.
func computeRegister(payrollMap map[string]PayrollRecord, timeMap map[string]TimeRecord, benefitsMap map[string]BenefitsRecord) []PayRegister {
	var registers []PayRegister

	for key, payroll := range payrollMap {
		timeRec, okTime := timeMap[key]
		benefitsRec, okBenefits := benefitsMap[key]
		if !okTime || !okBenefits {
			// Skip if any record is missing.
			continue
		}

		// Compute Gross Wages:
		// GrossWages = HourlyRate * RegularHours + 1.5 * HourlyRate * OvertimeHours
		grossWages := payroll.HourlyRate*float64(timeRec.RegularHours) +
			1.5*payroll.HourlyRate*float64(timeRec.OvertimeHours)

		// Compute Taxes
		federalTax := 0.12 * grossWages
		stateTax := 0.05 * grossWages
		socialSecurity := 0.062 * grossWages
		medicare := 0.0145 * grossWages

		// Total Benefits
		totalBenefits := benefitsRec.HealthInsurance + benefitsRec.Retirement + benefitsRec.OtherBenefits

		// Total Deductions = Taxes + Total Benefits
		totalDeductions := federalTax + stateTax + socialSecurity + medicare + totalBenefits

		// Net Pay
		netPay := grossWages - totalDeductions

		reg := PayRegister{
			EmployeeID:      payroll.EmployeeID,
			EmployeeName:    payroll.EmployeeName,
			JobTitle:        payroll.JobTitle,
			PayPeriod:       payroll.PayPeriod,
			HourlyRate:      payroll.HourlyRate,
			RegularHours:    timeRec.RegularHours,
			OvertimeHours:   timeRec.OvertimeHours,
			GrossWages:      grossWages,
			FederalTax:      federalTax,
			StateTax:        stateTax,
			SocialSecurity:  socialSecurity,
			Medicare:        medicare,
			HealthInsurance: benefitsRec.HealthInsurance,
			Retirement:      benefitsRec.Retirement,
			OtherBenefits:   benefitsRec.OtherBenefits,
			TotalBenefits:   totalBenefits,
			TotalDeductions: totalDeductions,
			NetPay:          netPay,
		}

		registers = append(registers, reg)
	}

	return registers
}

// writeRegister writes the computed pay register to a CSV file.
func writeRegister(registers []PayRegister, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("cannot create output file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"Employee ID", "Employee Name", "Job Title", "Pay Period", "Hourly Rate",
		"Regular Hours", "Overtime Hours", "Gross Wages", "Federal Tax", "State Tax",
		"Social Security", "Medicare", "Health Insurance", "Retirement", "Other Benefits",
		"Total Benefits", "Total Deductions", "Net Pay",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("cannot write header: %v", err)
	}

	// Write each record (format numbers to 2 decimals)
	for _, reg := range registers {
		row := []string{
			reg.EmployeeID,
			reg.EmployeeName,
			reg.JobTitle,
			reg.PayPeriod,
			fmt.Sprintf("%.2f", reg.HourlyRate),
			strconv.Itoa(reg.RegularHours),
			strconv.Itoa(reg.OvertimeHours),
			fmt.Sprintf("%.2f", reg.GrossWages),
			fmt.Sprintf("%.2f", reg.FederalTax),
			fmt.Sprintf("%.2f", reg.StateTax),
			fmt.Sprintf("%.2f", reg.SocialSecurity),
			fmt.Sprintf("%.2f", reg.Medicare),
			fmt.Sprintf("%.2f", reg.HealthInsurance),
			fmt.Sprintf("%.2f", reg.Retirement),
			fmt.Sprintf("%.2f", reg.OtherBenefits),
			fmt.Sprintf("%.2f", reg.TotalBenefits),
			fmt.Sprintf("%.2f", reg.TotalDeductions),
			fmt.Sprintf("%.2f", reg.NetPay),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("cannot write row: %v", err)
		}
	}

	return nil
}

func main() {
	// File names (adjust as needed)
	payrollFile := "payroll_data.csv"
	timeFile := "time_data.csv"
	benefitsFile := "benefits.csv"
	outputFile := "payroll_register.csv"

	// Start total timer.
	totalStart := time.Now()

	// Step 1: Read Input Files
	readStart := time.Now()
	payrollMap, err := readPayrollRecords(payrollFile)
	if err != nil {
		log.Fatalf("Error reading payroll records: %v", err)
	}

	timeMap, err := readTimeRecords(timeFile)
	if err != nil {
		log.Fatalf("Error reading time records: %v", err)
	}

	benefitsMap, err := readBenefitsRecords(benefitsFile)
	if err != nil {
		log.Fatalf("Error reading benefits records: %v", err)
	}
	readDuration := time.Since(readStart)
	fmt.Printf("Time to read input files: %v\n", readDuration)

	// Step 2: Compute the Pay Register
	computeStart := time.Now()
	registers := computeRegister(payrollMap, timeMap, benefitsMap)
	computeDuration := time.Since(computeStart)
	fmt.Printf("Time to compute pay register: %v\n", computeDuration)
	fmt.Printf("Computed %d register records.\n", len(registers))

	// Step 3: Write the Output CSV
	writeStart := time.Now()
	if err := writeRegister(registers, outputFile); err != nil {
		log.Fatalf("Error writing register file: %v", err)
	}
	writeDuration := time.Since(writeStart)
	fmt.Printf("Time to write output file: %v\n", writeDuration)

	// Total elapsed time
	totalDuration := time.Since(totalStart)
	fmt.Printf("Total elapsed time: %v\n", totalDuration)
	fmt.Printf("Pay register computed and saved to %s\n", outputFile)
}
