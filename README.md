# Legacy iOS Weather Server Concept

This server uses [Open-Meteo](https://open-meteo.com/) for weather data and [GeoNames](https://geonames.org/) for city names, latitude, and longitude.

**Note:** Only requests made by the iOS 6.0.1 type weather clients work atm. Also only added locations work. Support for the current location panel may be added later.  


## Prerequisites

- **Python 3**
- Install dependencies using pip:
  ```sh
  pip install flask requests_cache pandas openmeteo-requests retry_requests numpy
  ```


## Server Setup

### Step 1: Download the Server
Download and extract the server files into a folder of your choice.  
  
### Step 2: Download Location Data
Download the location data from [GeoNames](https://download.geonames.org/export/dump/allCountries.zip) and extract it into the same folder as the server.  
  
### Step 3: Process Location Data
Run the following script to process the location data:
  ```sh
  python 0_SimplifyAllCountries.py
  ```  
This just splits the massive allcountries.txt file into smaller text files based on the cities first letter.   
Once completed, you can delete **allCountries.txt**.  
  
### Step 4: Start the Server
Run the server script:
  ```sh
  python 1_Server.py
  ```

## Client Setup  

### Step 1: Connect on Legacy iOS
Connect to the server on your **legacy iOS device**.  
**Tested on:** iPhone 5 running **iOS 6.0.1**  
### Step 2: Remove existing locations  
Since the server uses geonameids instead of weoids you'll have to remove the old locations and add them again.  
## Projects Used  
This project uses parts of [YQL-X-Server-New](https://github.com/TestOrig/YQL-X-Server-New). Made by **Election** and **ObscureMosquito**
