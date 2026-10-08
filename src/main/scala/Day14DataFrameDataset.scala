import org.apache.spark.sql.SparkSession

case class Employee(
  id: Int,
  name: String,
  department: String,
  salary: Double
)

object Day14DataFrameDataset {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day14-DataFrame-Dataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("======================================")
    println("DAY 14 - DATAFRAME AND DATASET")
    println("Employee Payroll Pipeline")
    println("======================================")

    // STEP 1: Create Employee objects

    val employees = Seq(
      Employee(1, "Prabhat", "Data Engineering", 55000.0),
      Employee(2, "Rahul", "Data Engineering", 65000.0),
      Employee(3, "Amit", "HR", 45000.0),
      Employee(4, "Sneha", "Finance", 75000.0),
      Employee(5, "Priya", "Data Engineering", 85000.0),
      Employee(6,"aman","java developer",56000.0),
      Employee(7,"rohit","SdE",65700.0),
      Employee(8,"ruhi","Manger",150000.0)
    )

    // STEP 2: Convert Case Class -> Dataset

    val employeeDS = employees.toDS()

    println("\n===== DATASET =====")
    employeeDS.show()

    println("===== DATASET SCHEMA =====")
    employeeDS.printSchema()

    // STEP 3: Convert Dataset -> DataFrame

    val employeeDF = employeeDS.toDF()

    println("\n===== DATAFRAME =====")
    employeeDF.show()

    println("===== DATAFRAME SCHEMA =====")
    employeeDF.printSchema()

    // STEP 4: Filter using typed Dataset

    val highSalaryEmployees = employeeDS
      .filter(employee => employee.salary > 60000)

    println("\n===== HIGH SALARY EMPLOYEES =====")
    highSalaryEmployees.show()

    // STEP 5: Calculate 10% salary increment

    val employeesWithRaise = employeeDS.map { employee =>

      Employee(
        employee.id,
        employee.name,
        employee.department,
        employee.salary * 1.10
      )

    }

    println("\n===== SALARY AFTER 10% RAISE =====")
    employeesWithRaise.show()

    // STEP 6: Payroll calculation

    val payrollDF = employeeDS
      .map { employee =>

        (
          employee.id,
          employee.name,
          employee.department,
          employee.salary,
          employee.salary / 12
        )

      }
      .toDF(
        "id",
        "name",
        "department",
        "annual_salary",
        "monthly_salary"
      )

    println("\n===== EMPLOYEE PAYROLL =====")
    payrollDF.show()

    // STEP 7: Average salary by department

    val departmentSalary = employeeDF
      .groupBy("department")
      .avg("salary")

    println("\n===== AVERAGE SALARY BY DEPARTMENT =====")
    departmentSalary.show()

    // STEP 8: Catalyst execution plan

    println("\n===== CATALYST EXECUTION PLAN =====")

    employeeDF
      .filter($"salary" > 60000)
      .select("name", "salary")
      .explain(true)

    println("\n======================================")
    println("DAY 14 COMPLETED SUCCESSFULLY")
    println("======================================")

    spark.stop()
  }
}
