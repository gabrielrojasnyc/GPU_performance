import argparse
import time
import numpy as np
import pandas as pd

def simulate_complex_payroll_data_cpu(n_rows: int) -> pd.DataFrame:
    """
    Simulate a complex payroll dataset with n_rows using Pandas/NumPy.
    
    Columns include:
      - Employee Name
      - Employee ID
      - Job Title
      - Pay Period
      - Regular Hours
      - Overtime Hours
      - Gross Wages
      - Federal Tax
      - State Tax
      - Social Security
      - Medicare
      - Health Insurance
      - Retirement
      - Other Deductions
      - Net Pay
    """
    np.random.seed(42)
    
    # Generate string columns using NumPy random choice
    first_names = ["John", "Jane", "Alex", "Emily", "Michael", "Sarah", "David", "Laura"]
    last_names = ["Doe", "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller"]
    firsts = np.random.choice(first_names, n_rows)
    lasts = np.random.choice(last_names, n_rows)
    employee_names = [f"{f} {l}" for f, l in zip(firsts, lasts)]
    
    # Employee IDs (zero padded)
    id_width = max(3, len(str(n_rows)))
    employee_ids = [f"{i:0{id_width}d}" for i in range(1, n_rows + 1)]
    
    # Job Titles and Pay Periods
    job_titles = ["Manager", "Clerk", "Engineer", "Analyst", "Technician", 
                  "Salesperson", "Administrator", "Supervisor"]
    job_title_choices = np.random.choice(job_titles, n_rows)
    
    pay_periods = ["01/01-01/15", "01/16-01/31", "02/01-02/15", 
                   "02/16-02/28", "03/01-03/15", "03/16-03/31"]
    pay_period_choices = np.random.choice(pay_periods, n_rows)
    
    # Numeric columns with NumPy
    regular_hours = np.random.randint(70, 81, n_rows)
    overtime_hours = np.random.randint(0, 11, n_rows)
    base_rates = np.random.uniform(15, 50, n_rows)
    
    # Gross wages: regular + overtime (overtime at 1.5x rate)
    gross_wages = regular_hours * base_rates + overtime_hours * (1.5 * base_rates)
    
    # Deductions
    federal_rates = np.random.uniform(0.10, 0.15, n_rows)
    federal_tax = gross_wages * federal_rates
    
    state_rates = np.random.uniform(0.03, 0.06, n_rows)
    state_tax = gross_wages * state_rates
    
    social_security = gross_wages * 0.062
    medicare = gross_wages * 0.0145
    
    health_insurance = np.random.uniform(50, 100, n_rows)
    retirement = np.random.uniform(30, 70, n_rows)
    other_deductions = np.random.uniform(10, 30, n_rows)
    
    total_deductions = (federal_tax + state_tax + social_security +
                        medicare + health_insurance + retirement +
                        other_deductions)
    
    net_pay = gross_wages - total_deductions
    
    # Build the DataFrame (round monetary values to 2 decimals)
    df = pd.DataFrame({
        "Employee Name": employee_names,
        "Employee ID": employee_ids,
        "Job Title": job_title_choices,
        "Pay Period": pay_period_choices,
        "Regular Hours": regular_hours,
        "Overtime Hours": overtime_hours,
        "Gross Wages": np.round(gross_wages, 2),
        "Federal Tax": np.round(federal_tax, 2),
        "State Tax": np.round(state_tax, 2),
        "Social Security": np.round(social_security, 2),
        "Medicare": np.round(medicare, 2),
        "Health Insurance": np.round(health_insurance, 2),
        "Retirement": np.round(retirement, 2),
        "Other Deductions": np.round(other_deductions, 2),
        "Net Pay": np.round(net_pay, 2)
    })
    return df

def simulate_complex_payroll_data_gpu(n_rows: int):
    """
    Simulate a complex payroll dataset with n_rows using cuDF and CuPy.
    
    For numeric columns the data is generated with CuPy (on GPU).  
    For string columns (Employee Name, Employee ID, Job Title, Pay Period), 
    we generate them using NumPy (CPU) and then pass them to cuDF.
    """
    import cudf
    import cupy as cp

    # For reproducibility on GPU (CuPy)
    cp.random.seed(42)
    
    # Generate string columns using NumPy (CPU)
    first_names = ["John", "Jane", "Alex", "Emily", "Michael", "Sarah", "David", "Laura"]
    last_names = ["Doe", "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller"]
    firsts = np.random.choice(first_names, n_rows)
    lasts = np.random.choice(last_names, n_rows)
    employee_names = [f"{f} {l}" for f, l in zip(firsts, lasts)]
    
    id_width = max(3, len(str(n_rows)))
    # Use CuPy to generate IDs then convert to a list of formatted strings
    employee_ids_arr = cp.arange(1, n_rows + 1)
    employee_ids = [f"{int(i):0{id_width}d}" for i in cp.asnumpy(employee_ids_arr)]
    
    job_titles = ["Manager", "Clerk", "Engineer", "Analyst", "Technician", 
                  "Salesperson", "Administrator", "Supervisor"]
    # Using NumPy for random choice
    job_title_choices = np.random.choice(job_titles, n_rows)
    
    pay_periods = ["01/01-01/15", "01/16-01/31", "02/01-02/15", 
                   "02/16-02/28", "03/01-03/15", "03/16-03/31"]
    pay_period_choices = np.random.choice(pay_periods, n_rows)
    
    # Now generate numeric columns with CuPy
    regular_hours = cp.random.randint(70, 81, size=n_rows)
    overtime_hours = cp.random.randint(0, 11, size=n_rows)
    base_rates = cp.random.uniform(15, 50, size=n_rows)
    
    gross_wages = regular_hours * base_rates + overtime_hours * (1.5 * base_rates)
    
    federal_rates = cp.random.uniform(0.10, 0.15, size=n_rows)
    federal_tax = gross_wages * federal_rates
    
    state_rates = cp.random.uniform(0.03, 0.06, size=n_rows)
    state_tax = gross_wages * state_rates
    
    social_security = gross_wages * 0.062
    medicare = gross_wages * 0.0145
    
    health_insurance = cp.random.uniform(50, 100, size=n_rows)
    retirement = cp.random.uniform(30, 70, size=n_rows)
    other_deductions = cp.random.uniform(10, 30, size=n_rows)
    
    total_deductions = (federal_tax + state_tax + social_security +
                        medicare + health_insurance + retirement +
                        other_deductions)
    
    net_pay = gross_wages - total_deductions
    
    # Round monetary values (using CuPy) and then convert to CPU arrays for cuDF
    gross_wages = cp.round(gross_wages, 2)
    federal_tax = cp.round(federal_tax, 2)
    state_tax = cp.round(state_tax, 2)
    social_security = cp.round(social_security, 2)
    medicare = cp.round(medicare, 2)
    health_insurance = cp.round(health_insurance, 2)
    retirement = cp.round(retirement, 2)
    other_deductions = cp.round(other_deductions, 2)
    net_pay = cp.round(net_pay, 2)
    
    # Create a cuDF DataFrame (for numeric columns we use .get() to bring CuPy arrays to host)
    df = cudf.DataFrame({
        "Employee Name": employee_names,
        "Employee ID": employee_ids,
        "Job Title": job_title_choices.tolist(),
        "Pay Period": pay_period_choices.tolist(),
        "Regular Hours": cp.asnumpy(regular_hours),
        "Overtime Hours": cp.asnumpy(overtime_hours),
        "Gross Wages": cp.asnumpy(gross_wages),
        "Federal Tax": cp.asnumpy(federal_tax),
        "State Tax": cp.asnumpy(state_tax),
        "Social Security": cp.asnumpy(social_security),
        "Medicare": cp.asnumpy(medicare),
        "Health Insurance": cp.asnumpy(health_insurance),
        "Retirement": cp.asnumpy(retirement),
        "Other Deductions": cp.asnumpy(other_deductions),
        "Net Pay": cp.asnumpy(net_pay)
    })
    return df

def compute_net_pay_cpu(df: pd.DataFrame) -> pd.DataFrame:
    """
    (Optional) Recompute Net Pay for the CPU version to verify the deduction calculations.
    """
    df["Net Pay"] = (df["Gross Wages"] - (df["Federal Tax"] + df["State Tax"] +
                    df["Social Security"] + df["Medicare"] +
                    df["Health Insurance"] + df["Retirement"] +
                    df["Other Deductions"]))
    return df

def compute_cpu(n_rows: int):
    print("Running complex payroll computation on CPU using Pandas...")
    df = simulate_complex_payroll_data_cpu(n_rows)
    start_time = time.time()
    df = compute_net_pay_cpu(df)
    elapsed = time.time() - start_time
    print(f"CPU processing time for {n_rows} rows: {elapsed:.4f} seconds")
    print(f"Sample payroll data (first 5 rows):\n{df.head()}\n")
    return elapsed

def compute_gpu(n_rows: int):
    try:
        import cudf
    except ImportError:
        print("Error: RAPIDS cuDF is not installed. Please install cuDF to run the GPU version.")
        return None

    print("Running complex payroll computation on GPU using cuDF and CuPy...")
    start_time = time.time()
    df = simulate_complex_payroll_data_gpu(n_rows)
    
    # (Optionally) Recalculate Net Pay on GPU for verification.
    df["Net Pay"] = (df["Gross Wages"] - (df["Federal Tax"] + df["State Tax"] +
                     df["Social Security"] + df["Medicare"] +
                     df["Health Insurance"] + df["Retirement"] +
                     df["Other Deductions"]))
    # Bring a sample back to CPU for display
    result = df.head().to_pandas()
    elapsed = time.time() - start_time
    print(f"GPU processing time for {n_rows} rows: {elapsed:.4f} seconds")
    print(f"Sample payroll data (first 5 rows):\n{result}\n")
    return elapsed

def main():
    parser = argparse.ArgumentParser(
        description="Experiment: Compare CPU vs GPU performance for Complex Payroll Computation."
    )
    parser.add_argument(
        "--n_rows", type=int, default=1_000_000,
        help="Number of payroll records to simulate (default: 1,000,000)"
    )
    parser.add_argument(
        "--device", type=str, choices=['cpu', 'gpu', 'both'], default='both',
        help="Device to run the experiment on: 'cpu', 'gpu', or 'both' (default: both)"
    )
    args = parser.parse_args()

    if args.device in ['cpu', 'both']:
        cpu_time = compute_cpu(args.n_rows)
    if args.device in ['gpu', 'both']:
        gpu_time = compute_gpu(args.n_rows)
    
    if args.device == 'both' and gpu_time is not None:
        print("Summary:")
        print(f"CPU time: {cpu_time:.4f} seconds")
        print(f"GPU time: {gpu_time:.4f} seconds")
        speedup = cpu_time / gpu_time if gpu_time > 0 else float('inf')
        print(f"GPU Speedup: {speedup:.2f}x faster than CPU")

if __name__ == "__main__":
    main()
