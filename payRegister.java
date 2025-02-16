import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class PayRegisterCalculator {

    // Data structures for the three types of records.
    public static class PayrollRecord {
        String employeeID;
        String employeeName;
        String jobTitle;
        String payPeriod;
        double hourlyRate;
        public PayrollRecord(String employeeID, String employeeName, String jobTitle, String payPeriod, double hourlyRate) {
            this.employeeID = employeeID;
            this.employeeName = employeeName;
            this.jobTitle = jobTitle;
            this.payPeriod = payPeriod;
            this.hourlyRate = hourlyRate;
        }
    }

    public static class TimeRecord {
        String employeeID;
        String payPeriod;
        int regularHours;
        int overtimeHours;
        public TimeRecord(String employeeID, String payPeriod, int regularHours, int overtimeHours) {
            this.employeeID = employeeID;
            this.payPeriod = payPeriod;
            this.regularHours = regularHours;
            this.overtimeHours = overtimeHours;
        }
    }

    public static class BenefitsRecord {
        String employeeID;
        String payPeriod;
        double healthInsurance;
        double retirement;
        double otherBenefits;
        public BenefitsRecord(String employeeID, String payPeriod, double healthInsurance, double retirement, double otherBenefits) {
            this.employeeID = employeeID;
            this.payPeriod = payPeriod;
            this.healthInsurance = healthInsurance;
            this.retirement = retirement;
            this.otherBenefits = otherBenefits;
        }
    }

    // Utility: Create a join key from EmployeeID and PayPeriod.
    public static String makeKey(String employeeID, String payPeriod) {
        return employeeID + "|" + payPeriod;
    }

    // Parse one line from payroll_data.csv into a PayrollRecord.
    public static PayrollRecord parsePayrollRecord(String line) {
        if (line == null) return null;
        String[] tokens = line.split(",");
        if (tokens.length < 5) return null;
        String employeeID = tokens[0].trim();
        String employeeName = tokens[1].trim();
        String jobTitle = tokens[2].trim();
        String payPeriod = tokens[3].trim();
        double hourlyRate = Double.parseDouble(tokens[4].trim());
        return new PayrollRecord(employeeID, employeeName, jobTitle, payPeriod, hourlyRate);
    }

    // Parse one line from time_data.csv into a TimeRecord.
    public static TimeRecord parseTimeRecord(String line) {
        if (line == null) return null;
        String[] tokens = line.split(",");
        if (tokens.length < 4) return null;
        String employeeID = tokens[0].trim();
        String payPeriod = tokens[1].trim();
        int regularHours = Integer.parseInt(tokens[2].trim());
        int overtimeHours = Integer.parseInt(tokens[3].trim());
        return new TimeRecord(employeeID, payPeriod, regularHours, overtimeHours);
    }

    // Parse one line from benefits.csv into a BenefitsRecord.
    public static BenefitsRecord parseBenefitsRecord(String line) {
        if (line == null) return null;
        String[] tokens = line.split(",");
        if (tokens.length < 5) return null;
        String employeeID = tokens[0].trim();
        String payPeriod = tokens[1].trim();
        double healthInsurance = Double.parseDouble(tokens[2].trim());
        double retirement = Double.parseDouble(tokens[3].trim());
        double otherBenefits = Double.parseDouble(tokens[4].trim());
        return new BenefitsRecord(employeeID, payPeriod, healthInsurance, retirement, otherBenefits);
    }

    // This method performs a three-way merge join assuming all files are sorted by EmployeeID|PayPeriod.
    public static void mergeJoinAndCompute(String payrollFile, String timeFile, String benefitsFile, String outputFile) {
        long mergeStart = System.currentTimeMillis();
        try (BufferedReader brPayroll = new BufferedReader(new FileReader(payrollFile));
             BufferedReader brTime = new BufferedReader(new FileReader(timeFile));
             BufferedReader brBenefits = new BufferedReader(new FileReader(benefitsFile));
             PrintWriter pw = new PrintWriter(new FileWriter(outputFile))) {

            // Write header for output.
            pw.println("Employee ID,Employee Name,Job Title,Pay Period,Hourly Rate,Regular Hours,Overtime Hours,Gross Wages,Federal Tax,State Tax,Social Security,Medicare,Health Insurance,Retirement,Other Benefits,Total Benefits,Total Deductions,Net Pay");

            // Skip headers in input files.
            String payrollHeader = brPayroll.readLine();
            String timeHeader = brTime.readLine();
            String benefitsHeader = brBenefits.readLine();

            // Read the first record from each file.
            String payrollLine = brPayroll.readLine();
            String timeLine = brTime.readLine();
            String benefitsLine = brBenefits.readLine();

            PayrollRecord currentPayroll = parsePayrollRecord(payrollLine);
            TimeRecord currentTime = parseTimeRecord(timeLine);
            BenefitsRecord currentBenefits = parseBenefitsRecord(benefitsLine);

            while (currentPayroll != null && currentTime != null && currentBenefits != null) {
                String keyPayroll = makeKey(currentPayroll.employeeID, currentPayroll.payPeriod);
                String keyTime = makeKey(currentTime.employeeID, currentTime.payPeriod);
                String keyBenefits = makeKey(currentBenefits.employeeID, currentBenefits.payPeriod);

                // If all keys match, compute and write the output record.
                if (keyPayroll.equals(keyTime) && keyPayroll.equals(keyBenefits)) {
                    double grossWages = currentPayroll.hourlyRate * currentTime.regularHours +
                            1.5 * currentPayroll.hourlyRate * currentTime.overtimeHours;
                    double federalTax = 0.12 * grossWages;
                    double stateTax = 0.05 * grossWages;
                    double socialSecurity = 0.062 * grossWages;
                    double medicare = 0.0145 * grossWages;
                    double totalBenefits = currentBenefits.healthInsurance + currentBenefits.retirement + currentBenefits.otherBenefits;
                    double totalDeductions = federalTax + stateTax + socialSecurity + medicare + totalBenefits;
                    double netPay = grossWages - totalDeductions;

                    String outputLine = String.format("%s,%s,%s,%s,%.2f,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
                            currentPayroll.employeeID, currentPayroll.employeeName, currentPayroll.jobTitle, currentPayroll.payPeriod,
                            currentPayroll.hourlyRate, currentTime.regularHours, currentTime.overtimeHours, grossWages,
                            federalTax, stateTax, socialSecurity, medicare,
                            currentBenefits.healthInsurance, currentBenefits.retirement, currentBenefits.otherBenefits,
                            totalBenefits, totalDeductions, netPay);
                    pw.println(outputLine);

                    // Advance each file.
                    payrollLine = brPayroll.readLine();
                    timeLine = brTime.readLine();
                    benefitsLine = brBenefits.readLine();
                    currentPayroll = parsePayrollRecord(payrollLine);
                    currentTime = parseTimeRecord(timeLine);
                    currentBenefits = parseBenefitsRecord(benefitsLine);
                } else {
                    // Determine the smallest key lexicographically.
                    String minKey = keyPayroll;
                    if (keyTime.compareTo(minKey) < 0) minKey = keyTime;
                    if (keyBenefits.compareTo(minKey) < 0) minKey = keyBenefits;

                    if (minKey.equals(keyPayroll)) {
                        payrollLine = brPayroll.readLine();
                        currentPayroll = parsePayrollRecord(payrollLine);
                    }
                    if (minKey.equals(keyTime)) {
                        timeLine = brTime.readLine();
                        currentTime = parseTimeRecord(timeLine);
                    }
                    if (minKey.equals(keyBenefits)) {
                        benefitsLine = brBenefits.readLine();
                        currentBenefits = parseBenefitsRecord(benefitsLine);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long mergeEnd = System.currentTimeMillis();
        System.out.println("Time for merge join and computation: " + (mergeEnd - mergeStart) + " ms");
    }

    public static void main(String[] args) {
        // File names (adjust paths if necessary).
        String payrollFile = "payroll_data.csv";
        String timeFile = "time_data.csv";
        String benefitsFile = "benefits.csv";
        String outputFile = "payroll_register.csv";

        long totalStart = System.currentTimeMillis();
        mergeJoinAndCompute(payrollFile, timeFile, benefitsFile, outputFile);
        long totalEnd = System.currentTimeMillis();
        System.out.println("Total elapsed time: " + (totalEnd - totalStart) + " ms");
        System.out.println("Pay register computed and saved to " + outputFile);
    }
}
