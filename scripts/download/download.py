import cdsapi
import time

# Initialize the CDS API client
c = cdsapi.Client()

# Define years, pressure levels, and variables
years = list(range(2024, 2025))
pressure_levels = ["350", "400", "450", "500", "550", "600", "650", "700", "750", "800", "850", "900","950", "1000"]  # Modify pressure levels as needed
variables = ["specific_humidity", "u_component_of_wind", "v_component_of_wind"]

# Max number of retries for failed downloads
MAX_RETRIES = 5

# Loop through years and split into two parts (Jan-Jun & Jul-Dec)
for year in years:
    for part, months in enumerate([["01", "02", "03"],["04", "05", "06"], ["07", "08", "09"], ["10", "11", "12"]]):
        file_name = f"{year}_part{part+1}.nc"  # Output file name

        print(f"Requesting {file_name}...")

        for attempt in range(1, MAX_RETRIES + 1):  # Retry up to 5 times
            try:
                # Make the request
                c.retrieve(
                    "reanalysis-era5-pressure-levels",
                    {
                        "product_type": "reanalysis",
                        "format": "netcdf",
                        "variable": variables,
                        "pressure_level": pressure_levels,
                        "year": str(year),
                        "month": months,
                        "day": [1,15],  
                        "time": ["00:00","08:00","16:00"], 
                        "download_format": "unarchived",
                        "data_format": "netcdf"
                    },
                    file_name,  
                )

                print(f"✅ Successfully downloaded: {file_name}")
                break  # Exit retry loop if download succeeds

            except Exception as e:
                print(f"⚠️ Attempt {attempt}/{MAX_RETRIES} failed for {file_name}: {e}")
                if attempt < MAX_RETRIES:
                    print("Retrying in 10 seconds...")
                    time.sleep(10)  # Wait before retrying
                else:
                    print(f"❌ Failed to download {file_name} after {MAX_RETRIES} attempts.")