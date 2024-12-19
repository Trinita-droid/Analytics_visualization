import pandas as pd # pandas and numpy for working with array and dataframe structures
import numpy as np 
import xml.etree.ElementTree as ET # ElementTree module provides class and functions to work with XML data structure
from pymongo import MongoClient # to interact with mongodb in python
import requests #allows to send HTTPs requests in python to interact with web-APIs
import warnings #to supress unnecessary warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt #python basic visualization
import seaborn as sns #to create complex visualizations
import psycopg2 #to work with postgresql databases in python
from psycopg2.extras import execute_values

# fetch the API containing XML data using requests library and raise status when encountered error
xml_url = "https://data.cityofchicago.org/api/views/7ce8-bpr6/rows.xml?accessType=DOWNLOAD"
response=requests.get(xml_url)
response.raise_for_status()

# class encapsulating methods for whole project
class encap_process():
    """initializing the class attributes to be accessible across methods"""
    def __init__(self) -> None:
        self.tree = None
        self.rows = []
        self.tags = set()
        self.xml_raw_df = pd.DataFrame()
        self.na_df = pd.DataFrame()

    def xml_parse(self):
        """function to parse the xml file, create a tree like structure to access
         the root element and return the number of rows present in the original dataset"""
        try:
            parse_xml = ET.ElementTree(ET.fromstring(response.content))
            self.tree = parse_xml.getroot()
            self.rows = self.tree.findall(".//row")
        except Exception as e:
            print(f"Error while parsing the xml file : {e}")
        finally:
            print(f"Total number of rows in the dataset = {len(self.rows)}\n")

    def xml_tags(self):
        """method to iter through all the root elements tags and 
        add the unique values in set which is then printed"""
        try:
            for elem in self.tree.iter():
                self.tags.add(elem.tag)
        except Exception as e:
            print(f"Error while fetching the xml tags : {e}")
        finally:
            print(f"Tags in the data : {self.tags}\n")
    
    def xml_mongodb(self):
        """method to store the raw xml data into mongodb container called `mongodb_cont_2` with 
        database named `xml_mongodb2` and table/collection `xml_collection` created using docker"""
        client = MongoClient("mongodb://localhost:27018")
        db_name = client["xml_mongodb2"]
        collection = db_name["xml_collection"]
        try:
            data = []  # List to store the data to insert into MongoDB
            for row in self.rows:
                row_data = {}  # Dictionary to hold data for each row
                for child in row:
                    row_data[child.tag] = child.text  # Map the XML tag to its text content
                data.append(row_data)

            if data:
                # proceed to insert the data into the MongoDB collection if the data is collected in list
                collection.insert_many(data)
                print(f"Successfully inserted {len(data)} records into MongoDB collection '{collection.name}'")
            else:
                print("No data found to insert.")
        except Exception as e:
            print(f"Error while inserting data into MongoDB: {e}")

    def xml_pd(self):
        """method to convert the list of parsed xml data into dataframe"""
        raw_df = []
        for row in self.rows:
            data = {child.tag:child.text for child in row}
            raw_df.append(data)
        self.xml_raw_df = pd.DataFrame(raw_df)
        print(f"Raw df from XML:\n {self.xml_raw_df.head()}\n")

    def missing_vals(self):
        # checks if the "row" element is found from the previous function run, if nothing is rendered error message is printed.
        if not self.rows:
            print("No rows avail. Run func1 first")
        # initializing an empty list to store missing vals
        missing_vals = []
        """using dictionary comprehension, access the child element's tag and text
           by looping into each of row elements again.    
       """ 
        for index, row in enumerate(self.rows):
            row_data = { child.tag : child.text  for child in row}
            # since it is dict, items are accessed to check whether the values are empty
            missing_fields = [tag for tag, value in row_data.items() if value is None or value =='']
            # if values are found to be null, these row indexes are appended to the list with index and fields.
            if missing_fields:
                missing_vals.append({'row_index': index, 'missing_fields' : missing_fields})
        # another iteration is done into the list of missing vals, that prints out each row index with missing field.
        if missing_vals:
            print("Missing values found :")
            for info in missing_vals:
                print(f"Row {info['row_index']} is missing fields: {info['missing_fields']}")
        else:
            print("No missing values found in the xml data")

    def preprocess_vals(self,):
        # replacing the missing vals with string "Unknown"
        raw_df = self.xml_raw_df.copy()
        if raw_df.empty:
            print("EMPTY DF")
        print(raw_df.isnull().sum())
        # self.na_df = raw_df.fillna("Unknown")
        # print("cleaned_df \n ",self.na_df.head())
        # print(self.na_df.isnull().sum())
        self.na_df = raw_df

        # converting columns like percent and week to numeric
        print(f"Data types before conversion \n {self.na_df.dtypes}")

        # Convert to datetime
        self.na_df['week_start'] = pd.to_datetime(self.na_df['week_start'], errors='coerce')
        self.na_df['week_end'] = pd.to_datetime(self.na_df['week_end'], errors='coerce')
        self.na_df['current_week_ending'] = pd.to_datetime(self.na_df['current_week_ending'], errors='coerce')

        # Convert to category
        categorical_columns = [
            "season", 
            "data_source", 
            "essence_category", 
            "respiratory_category", 
            "visit_type", 
            "demographic_category", 
            "demographic_group"
        ]
        self.na_df[categorical_columns] = self.na_df[categorical_columns].astype('category')

        # Check updated data types
        print(self.na_df.dtypes)

    def xml_postgres(self):
        """method to insert the processed data into postgresql database with connection strings like
        host name, db name `"""
        connection = None
        try:
            connection = psycopg2.connect(
                host = "localhost",
                port = "5433",
                database = "xml_postgres2",
                user = "dap_user",
                password = "Mypostgresql"
            )
            print(f"Connected to the database {connection.get_dsn_parameters()['dbname']} in docker container postgres_cont_2")
        except Exception as e:
            print(f"Error in connecting to docker postgre container : {e}")
        
        try:
            # Establish cursor connection
            cursor = connection.cursor()

            # Create table query
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS respiratory_illness (
                mmwr_week TEXT,
                week TEXT,
                week_start DATE,
                week_end DATE,
                season TEXT,
                data_source TEXT,
                essence_category TEXT,
                respiratory_category TEXT,
                visit_type TEXT,
                demographic_category TEXT,
                demographic_group TEXT,
                percent NUMERIC,
                current_week_ending DATE
            );
            '''
            cursor.execute(create_table_query)
            connection.commit()

            # Insert query
            insert_query = '''
            INSERT INTO respiratory_illness (
                mmwr_week, week, week_start, week_end, season,
                data_source, essence_category, respiratory_category,
                visit_type, demographic_category, demographic_group, 
                percent, current_week_ending
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''

            # Iterate over rows of the DataFrame
            for _, row in self.na_df.iterrows():
                # Prepare data, replace NaT or NaN with None
                data_tuple = (
                    row['mmwr_week'], row['week'],
                    row['week_start'] if pd.notna(row['week_start']) else None,
                    row['week_end'] if pd.notna(row['week_end']) else None,
                    row['season'], row['data_source'], 
                    row['essence_category'], row['respiratory_category'], 
                    row['visit_type'], row['demographic_category'], 
                    row['demographic_group'], 
                    row['percent'] if pd.notna(row['percent']) else None,
                    row['current_week_ending'] if pd.notna(row['current_week_ending']) else None
                )

                # Execute the query
                cursor.execute(insert_query, data_tuple)
                connection.commit()  # Commit after each insert

            print(f"\nInserted {len(self.na_df)} records into PostgreSQL")

        except Exception as e:
            print(f"Error while inserting the data into docker-postgres: {e}")

        finally:
            if connection:
                cursor.close()
                connection.close()  

    def viz1_season_visit(self):
    # Stacked bar chart of 'season' vs 'visit_type'
        self.na_df.groupby(['season', 'visit_type']).size().unstack().plot(kind='bar', stacked=True, figsize=(10, 6))

        plt.title('Season vs Visit Type (Stacked Bar Chart)')
        plt.ylabel('Count')
        plt.xlabel('Season')
        plt.show()

    def viz2_pivot_season(self):
        # Create a pivot table to show 'season' for each 'season_year'
        pivot_table = self.na_df.pivot_table(index='season', columns='season', aggfunc='size', fill_value=0)
    # Display the pivot table
        print(pivot_table)

    def viz3_distro_season_year(self):

# Create a count plot to show the distribution of 'season' by 'season_year'
        plt.figure(figsize=(10, 6))
        sns.countplot(data=self.na_df, x='season', hue='season')
        plt.title("Distribution of Seasons by Year")
        plt.xlabel("Season Year")
        plt.ylabel("Count")
        plt.show()

        grouped_data = self.na_df.groupby(["visit_type", "demographic_group"]).size().reset_index(name="count")

        # Advanced Visualization: Grouped Bar Plot
        plt.figure(figsize=(14, 8))  # Set the figure size
        sns.set(style="whitegrid")

        # Create the barplot
        sns.barplot(data=grouped_data, x="demographic_group", y="count", hue="visit_type", palette="viridis")

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=90)

        # Add plot title and labels
        plt.title("Comparison of Visit Types Across Demographic Groups", fontsize=16)
        plt.xlabel("Demographic Group", fontsize=12)
        plt.ylabel("Count of Visits", fontsize=12)

        # Show legend and the plot
        plt.legend(title="Visit Type")
        plt.tight_layout()
        plt.show()
        plt.savefig("Visit Types Across Demographic Groups")


if __name__=="__main__":

    xml_class=encap_process()
    xml_class.xml_parse()
    xml_class.xml_tags()
    xml_class.xml_mongodb()
    xml_class.xml_pd()
    xml_class.missing_vals()
    xml_class.preprocess_vals()
    xml_class.xml_postgres()
    xml_class.viz1_season_visit()
    xml_class.viz2_pivot_season()
    xml_class.viz3_distro_season_year()


        



    