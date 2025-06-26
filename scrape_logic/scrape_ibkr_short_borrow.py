import ftplib
import pandas as pd
import os

# FTP server details
FTP_SERVER = "ftp2.interactivebrokers.com"
FTP_USERNAME = "shortstock"
FTP_PASSWORD = ""  # No password required

# File to download (replace with the correct filename)
FILENAME = "usa.txt"  # Example for US stocks

def download_ftp_file(server, username, password, filename):
    """
    Connects to the FTP server and downloads the specified file.
    """
    try:
        # Connect to the FTP server
        ftp = ftplib.FTP(server)
        ftp.login(username, password)
        print("Connected to FTP server.")

        # Download the file
        with open(filename, "wb") as file:
            ftp.retrbinary(f"RETR {filename}", file.write)
        print(f"Downloaded {filename} successfully.")

        # Close the FTP connection
        ftp.quit()
    except Exception as e:
        print(f"Error downloading file: {e}")

def parse_stock_loan_data(filename):
    """
    Parses the stock loan data file and extracts borrow fees and available shares.
    """
    try:
        # Read the file into a pandas DataFrame
        # Skip the first line (comment) and use the second line as the header
        df = pd.read_csv(filename, delimiter="|", skiprows=1, header=0)

        # Assign column names (based on the file structure)
        df.columns = [
            "Symbol", "Currency", "Name", "IB Contract ID", "ISIN",
            "Rebate Rate", "Fee Rate", "Available Shares", "Unnamed"  # Last column is empty
        ]

        # Drop the last column (empty) and any rows with missing data
        df = df.drop(columns=["Unnamed"])
        df = df.dropna(subset=["Fee Rate", "Available Shares"])

        # Convert relevant columns to numeric
        df["Fee Rate"] = pd.to_numeric(df["Fee Rate"], errors="coerce")
        df["Available Shares"] = pd.to_numeric(df["Available Shares"], errors="coerce")

        return df
    except Exception as e:
        print(f"Error parsing file: {e}")
        return pd.DataFrame()

def main():
    # Step 1: Download the stock loan data file from the FTP server
    download_ftp_file(FTP_SERVER, FTP_USERNAME, FTP_PASSWORD, FILENAME)

    # Step 2: Parse the downloaded file
    stock_loan_data = parse_stock_loan_data(FILENAME)

    if not stock_loan_data.empty:
        # Step 3: Display the data (or save it to a CSV)
        output_folder = "data_upload"
        output_filename = os.path.join(output_folder, "stock_loan_data.csv")

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        print(stock_loan_data[["Symbol", "Fee Rate", "Available Shares"]])
        stock_loan_data.to_csv(output_filename, index=False)
        print(f"Data saved to {output_filename}")
    else:
        print("No data found or an error occurred.")

if __name__ == "__main__":
    main()
