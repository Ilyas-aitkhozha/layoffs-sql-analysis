World Layoffs: Data Cleaning and EDA with MySQL

This project contains a complete Data Cleaning and Exploratory Data Analysis (EDA) process on a real-world dataset related to layoffs around the world, using **pure SQL in MySQL**.

---

 Project Goals

- Clean the raw `world_layoffs` dataset step-by-step.
- Standardize and structure the data for analysis.
- Conduct insightful EDA using only SQL.
- Make the project reusable and easy to build upon (future goal: add SQL functions & procedures).

---

Project Structure


---

Steps Performed (Data Cleaning)

1. **Remove Duplicates**
   - Used `ROW_NUMBER()` with `PARTITION BY` to identify and remove duplicates.

2. **Standardize Data**
   - Trimmed whitespace.
   - Merged industry names (e.g., "Crypto", "Cryptocurrency").
   - Fixed country names ("United States." → "United States").
   - Converted `date` from text to `DATE` format.

3. **Handle Null or Blank Values**
   - Replaced blank strings with `NULL`.
   - Forward-filled missing values from similar rows (same `company`).
   - Deleted rows with no useful info (`total_laid_off` and `percentage_laid_off` both `NULL`).

4. **Drop Unnecessary Columns**
   - Removed `row_num` column after duplicate handling.

---

 Exploratory Data Analysis (EDA)

Some of the insights extracted:
- Top companies and industries with the highest layoffs.
- Monthly trends and rolling totals.
- Impact per country, stage, and year.
- Use of CTEs and window functions (`DENSE_RANK()`, `SUM() OVER()` etc).

---

Future changes

- Take data, how many employees were in this companies before laid off and do calculations with them
- Possibly use parts of this project in Python (with Pandas framework)

