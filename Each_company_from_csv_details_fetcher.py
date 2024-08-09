import requests
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    filename='company_data_fetch.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

base_api = 'https://partnerfinder.sap.com/sap/details/api/partnerProfile/findByPartnerProfileId/'

def fetch_details(profile_id):
    url = base_api + profile_id
    try:
        response = requests.get(url)
        if response.status_code == 200:
            response.raise_for_status()
            data = response.json()
            logging.info(f"Successfully fetched data for profile_id: {profile_id}")
            return data
        else:
            logging.error(f"API failed with status code: {response.status_code} for profile_id: {profile_id}")
            return {}
    except requests.exceptions.RequestException as r:
        logging.error(f'HTTP error occurred for profile_id {profile_id}: {r}')
    except Exception as err:
        logging.error(f'Other error occurred for profile_id {profile_id}: {err}')
    return None

def get_address_specialization(each_company_data):
    if each_company_data is None:
        return "No data available"

    local_profiles = each_company_data.get('localProfiles', [])
    competencies = each_company_data.get('competencies', [])

    # Initialize dictionaries
    company_address = {}
    company_specialization = {}

    for specialization in competencies:
        name = specialization.get('name', 'Unknown')
        company_specialization[name] = {
            'level': specialization.get('level', 'Unknown'),
            'numberOfSolutions1': specialization.get('solutionL1Key', 0),
            'numberOfSolutions2': specialization.get('solutionL2Key', 0),
            'numberOfSolutions3': specialization.get('solutionL3Key', 0),
            'numberOfSolutions4': specialization.get('solutionL4Key', 0),
            'numberOfSolutions5': specialization.get('solutionL5Key', 0)
        }

    for profile in local_profiles:
        addresses = profile.get('addresses', [])
        country_name = profile.get('countryName', 'Unknown Country')

        if country_name not in company_address:
            company_address[country_name] = []

        if addresses:
            for address in addresses:
                company_address[country_name].append({
                    'addressLines': address.get('addressLines'),
                    'fullAddress': address.get('fullAddress'),
                    'city': address.get('city'),
                    'countryCode': address.get('countryCode'),
                    'regionText': address.get('regionText'),
                    'phone': address.get('phone'),
                    'email': address.get('email')
                })
        else:
            company_address[country_name].append({
                'addressLines': None,
                'fullAddress': None,
                'city': None,
                'countryCode': None,
                'regionText': None,
                'phone': None,
                'email': None
            })

    return {
        'company_address': company_address,
        'specializations': company_specialization
    }

def individual_company_fetch(pid):
    company_data = fetch_details(pid)
    json_pair = get_address_specialization(company_data)

    address = json_pair.get('company_address', {})
    specializations = json_pair.get('specializations', {})

    # Prepare combined data for DataFrame
    combined_data = []
    for country, addresses in address.items():
        for addr in addresses:
            for specialization, details in specializations.items():
                combined_data.append({
                    'Profile ID': pid,
                    'Country': country,
                    'Full Address': addr.get('fullAddress', 'N/A'),
                    'City': addr.get('city', 'N/A'),
                    'Region': addr.get('regionText', 'N/A'),
                    'Phone': addr.get('phone', 'N/A'),
                    'Email': addr.get('email', 'N/A'),
                    'Specialization': specialization,
                    'Level': details['level'],
                    'Solutions 1': details['numberOfSolutions1'],
                    'Solutions 2': details['numberOfSolutions2'],
                    'Solutions 3': details['numberOfSolutions3'],
                    'Solutions 4': details['numberOfSolutions4'],
                    'Solutions 5': details['numberOfSolutions5']
                })

    return combined_data

# Read the existing data from Fetched_data.csv
fetched_data = pd.read_csv(r'files/Fetched_data.csv')

# Initialize list to collect all combined data
all_combined_data = []

# Initialize counters
success_count = 0
failure_count = 0

for index, row in fetched_data.iterrows():
    pid = str(row['profileId']).zfill(10)
    print(f"Fetching data for Profile ID: {pid}")
    combined_data = individual_company_fetch(pid)

    if combined_data:
        success_count += 1
        # Add additional data from the original CSV to each entry
        for data in combined_data:
            data['Title'] = row['title']
            data['Competencies'] = row['competencies']
            data['Has Grow With SAP'] = row['hasGrowWithSap']
            data['Consultants'] = row['consultants']
            data['Description'] = row['description']

            # Only break after adding data for the first address to avoid duplicate entries
            break

        all_combined_data.extend(combined_data)
    else:
        failure_count += 1

# Create a DataFrame and save to a single CSV
combined_df = pd.DataFrame(all_combined_data)
combined_df.to_csv('combined_company_data.csv', index=False)

# Log the results
logging.info(f"Data fetching complete. Success: {success_count}, Failures: {failure_count}")
print(f"Data fetching complete. Success: {success_count}, Failures: {failure_count}")
print("Data has been saved to a single CSV file: combined_company_data.csv.")
