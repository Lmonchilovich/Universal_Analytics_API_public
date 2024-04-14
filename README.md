### **Description**

This project is a Python-based script for pulling and analyzing Universal Analytics data for a specified period. It is designed to automate the retrieval of monthly traffic reports from Google Analytics using various filters, such as organic traffic, blog traffic, and combinations thereof. The tool can generate reports for specific date ranges (monthly by default) and supports extensions for daily, weekly, and custom reporting intervals.

### **Setup and Installation**

To get this script running on your machine, follow these steps:

1. **Clone the Repository**
    
    ```python
    
    git clone [<repository-url>](https://github.com/Lmonchilovich/Universal_Analytics_API_public.git)
    
    ```
    
2. **Install Required Libraries**
    - Ensure Python 3 is installed on your machine.
    - Install required Python packages:
        
        ```python
       
        pip install -r requirements.txt
        
        ```
        
3. **Authentication and Configuration**
    - You will need access to Google Analytics API and Google BigQuery (if using BigQuery as your data source).
    - Obtain a Google service account key JSON file and place it in your project directory. Update the **`KEY_FILE_LOCATION`** in the script with the path to this file.
    - If you choose not to use BigQuery, prepare a CSV file or a list of tuples with domains and view IDs. Modify the **`fetch_ids()`** function to load this data instead of querying BigQuery.
4. **Setting up the Date Ranges**
    - Specify the start and end date in the script to define the overall period for which you want to generate reports. For instance, for the year 2022, set **`start_date = datetime.date(2022, 1, 1)`** and **`end_date = datetime.date(2022, 12, 31)`**.
5. **Running the Script**
    - Execute the main script to start data retrieval and analysis:
        
        ```python
        
        python <script-name>.py
        
        ```
        

### **Usage Notes**

- The tool is by default igured to handle monthly data grandefault. It iterates over each month within the specified date range, generating a separate report for each.
- The metric of 'users' is calculated distinctively across the specified range. Splitting this range or aggregating from smaller units (like days or landing pages) to a month may lead to inaccuracies due to session overlaps.

### **Advanced Customization**
- Custom reports and filters are also supported. I have also set up daily and weekly versions and added dimensions, such as the landing page and page path.
