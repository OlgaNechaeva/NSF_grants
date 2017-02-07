from lxml import etree
import os
import pandas as pd
import argparse
import psycopg2
from sqlalchemy import create_engine

# Connecting to the database
engine = create_engine('postgresql://olga:Penguin.92@192.168.2.26:5432/grants')
DB = {
    'drivername': 'postgres',
    'database': 'grants',
    'host': '192.168.2.26',
    'port': '5432',
    'username': 'olga',
    'password': 'Penguin.92'
}
dsn = "host={} dbname={} user={} password={}".format(DB['host'],
                                                     DB['database'],
                                                     DB['username'],
                                                     DB['password'])
conn = psycopg2.connect(dsn)
cur = conn.cursor()

# Setting of lists we will put in tables
IDs = []
# The elements of Grants Main Table
Awards = []
AwardTitle = []
AwardEffectiveDate = []
AwardID = []
AwardExpirationDate = []
AwardAmount = []
MinAmdLetterDate = []
MaxAmdLetterDate = []
AbstractNarration = []


# The elements of Instruments Table
Instruments = []
InstrumentValue = []

# The elements of Organisations Table
Organizations = []
OrganizationCode = []
OrganizationDirectorateLongName = []
OrganizationDivisionLongName = []

# The elements of ProgramOfficers Table
ProgramOfficers = []
ProgramOfficer = []

# The elements of Investigators Table
Investigators = []
InvestigatorFirstName = []
InvestigatorLastName = []
InvestigatorEmailAddress = []
InvestigatorStartDate = []
InvestigatorEndDate = []
InvestigatorRoleCode = []

# The elements of Institutions Table
Institutions = []
InstitutionName = []
InstitutionCityName = []
InstitutionZipCode = []
InstitutionPhoneNumber = []
InstitutionStreetAddress = []
InstitutionCountryName = []
InstitutionStateName = []
InstitutionStateCode = []

# The elements of ProgramElements Table
ProgramElements = []
ProgramElementCode = []
ProgramElementText = []

# The elements of ProgramReference Table
ProgramReferences = []
ProgramReferenceCode = []
ProgramReferenceText = []

# The elements of FoaIformation Table
FoaInformations = []
FoaInformationCode = []
FoaInformationName = []

# There is an option here. It is up to you, either to type the path here in the script
# or to run the script in the Terminal where you can enter the path like a string as well.
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path_out", help="You should type here the path to your directory where xml documents are.")
args = parser.parse_args()
path = args.path_out
# path = '/home/user/Downloads/1989'


def parse_elem(xpath):
    element = []
    elem = parser_file.xpath(xpath)
    if len(elem) == 1:
        element = elem
    elif len(elem) == 0:
        element = ''
    return element


def parse_more_elem(xpath):
    elements = []
    elems = parser_file.xpath(xpath)
    if len(elems) == 1:
        elements = elems
    elif len(elems) == 0:
        elements = ''
    elif len(elems) > 1:
        for inst in range(0, len(elems)):
            elements.append(elems[(inst)])
    return elements


for filename in os.listdir(path):
    if not filename.endswith('.xml'): continue
    fullname = os.path.join(path, filename)
    parser_file = etree.parse(fullname)

# The extraction of Awards elements from the directory fixed above (path)
    AwardTitle = parse_elem('//AwardTitle/text()')
    AwardEffectiveDate = parse_elem('//AwardEffectiveDate/text()')
    AwardExpirationDate = parse_elem('//AwardExpirationDate/text()')
    AwardAmount = parse_elem('//AwardAmount/text()')
    AbstractNarration = parse_elem('//AbstractNarration/text()')
    MinAmdLetterDate = parse_elem('//MinAmdLetterDate/text()')
    MaxAmdLetterDate = parse_elem('//MaxAmdLetterDate/text()')
    AwardID = parse_elem('//AwardID/text()')

# The extraction of Organisations elements from the directory
    OrganizationCode = parse_more_elem('//Organization/Code/text()')
    OrganizationDirectorateLongName = parse_more_elem('//Organization/Directorate/LongName/text()')
    OrganizationDivisionLongName = parse_more_elem('//Organization/Division/LongName/text()')

# The extraction of Instruments elements from the directory
    InstrumentValue = parse_more_elem('//AwardInstrument/Value/text()')

# The extraction of ProgramOfficers elements from the directory
    ProgramOfficer = parse_more_elem('//ProgramOfficer/SignBlockName/text()')

# The extraction of Investigators elements from the directory
    InvestigatorFirstName = parse_more_elem('//Investigator/FirstName/text()')
    InvestigatorLastName = parse_more_elem('//Investigator/LastName/text()')
    InvestigatorEmailAddress = parse_more_elem('//Investigator/EmailAddress/text()')
    InvestigatorStartDate = parse_more_elem('//Investigator/StartDate/text()')
    InvestigatorEndDate = parse_more_elem('//Investigator/EndDate/text()')
    InvestigatorRoleCode = parse_more_elem('//Investigator/RoleCode/text()')

# The extraction of Institutions Table elements
    InstitutionName = parse_elem('//Institution/Name/text()')
    InstitutionCityName = parse_elem('//Institution/CityName/text()')
    InstitutionZipCode = parse_elem('//Institution/ZipCode/text()')
    InstitutionPhoneNumber = parse_elem('//Institution/PhoneNumber/text()')
    InstitutionStreetAddress = parse_elem('//Institution/StreetAdress/text()')
    InstitutionCountryName = parse_elem('//Institution/CountryName/text()')
    InstitutionStateName = parse_elem('//Institution/StateName/text()')
    InstitutionStateCode = parse_elem('//Institution/StateCode/text()')

# The extraction of ProgramElement Table elements
    ProgramElementCode = parse_more_elem('//ProgramElement/Code/text()')
    ProgramElementText = parse_more_elem('//ProgramElement/Text/text()')

# The extraction of ProgramElement Table elements
    ProgramReferenceCode = parse_more_elem('//ProgramReference/Code/text()')
    ProgramReferenceText = parse_more_elem('//ProgramElement/Text/text()')

# The extraction of FoaInformation Table elements
    FoaInformationCode = parse_more_elem('//FoaInformation/Code/text()')
    FoaInformationName = parse_more_elem('//FoaInformation/Name/text()')

# Creating the lists we are going to put into database
    Awards.append({'award_title': '|'.join(AwardTitle),
                   'award_effective_date': '|'.join(AwardEffectiveDate),
                   'award_expiration_date': '|'.join(AwardExpirationDate),
                   'award_amount': '|'.join(AwardAmount),
                   'abstract_narration': '|'.join(AbstractNarration),
                   'min_amd_letter_date': '|'.join(MinAmdLetterDate),
                   'max_amd_letter_date': '|'.join(MaxAmdLetterDate),
                   'award_id': '|'.join(AwardID)})
    Organizations.append({'org_code': '|'.join(OrganizationCode),
                          'org_directorate': '|'.join(OrganizationDirectorateLongName),
                          'org_division': '|'.join(OrganizationDivisionLongName),
                          'award_id': '|'.join(AwardID)})
    Instruments.append({'instr_values': '|'.join(InstrumentValue), 'award_id': '|'.join(AwardID)})
    ProgramOfficers.append({'progr_officers': '|'.join(ProgramOfficer), 'award_id': '|'.join(AwardID)})
    Investigators.append({'invest_first_names': '|'.join(InvestigatorFirstName),
                                'invest_last_names': '|'.join(InvestigatorLastName),
                                'invest_email_addresses': '|'.join(InvestigatorEmailAddress),
                                'invest_start_dates': '|'.join(InvestigatorStartDate),
                                'invest_end_dates': '|'.join(InvestigatorEndDate),
                                'invest_role_codes': '|'.join(InvestigatorRoleCode),
                                'award_id': '|'.join(AwardID)})
    Institutions.append({'instit_names': '|'.join(InstitutionName),
                                'instit_city_names': '|'.join(InstitutionCityName),
                                'instit_zip_codes': '|'.join(InstitutionZipCode),
                                'instit_phone_numbers': '|'.join(InstitutionPhoneNumber),
                                'instit_street_addresses': '|'.join(InstitutionStreetAddress),
                                'instit_countries_names': '|'.join(InstitutionCountryName),
                                'instit_state_names': '|'.join(InstitutionStateName),
                                'instit_state_codes': '|'.join(InstitutionStateCode),
                                'award_id': '|'.join(AwardID)})
    ProgramElements.append({'progr_elem_codes': '|'.join(ProgramElementCode),
                                  'progr_elem_texts': '|'.join(ProgramElementText),
                                  'award_id': '|'.join(AwardID)})
    ProgramReferences.append({'progr_ref_codes': '|'.join(ProgramReferenceCode),
                                    'progr_ref_texts': '|'.join(ProgramReferenceText),
                                    'award_id': '|'.join(AwardID)})
    FoaInformations.append({'foa_inf_codes': '|'.join(FoaInformationCode),
                                    'foa_inf_names': '|'.join(FoaInformationName),
                                    'award_id': '|'.join(AwardID)})
    IDs.append({'award_id': '|'.join(AwardID)})

print(Awards)
print(len(Awards))
print(Instruments)
print(len(Instruments))
print(ProgramOfficers)
print(len(ProgramOfficers))
print(Investigators)
print(len(Investigators))
print(Institutions)
print(len(Institutions))
print(ProgramElements)
print(len(ProgramElements))
print(ProgramReferences)
print(len(ProgramReferences))
print(FoaInformations)
print(len(FoaInformations))
print(Organizations)
print(len(Organizations))
print(IDs)
print(len(IDs))

# Creating DataFrames for each table
table11 = pd.DataFrame(Awards)
table12 = pd.DataFrame(Instruments)
table13 = pd.DataFrame(ProgramOfficers)
table14 = pd.DataFrame(Investigators)
table17 = pd.DataFrame(Institutions)
table15 = pd.DataFrame(ProgramElements)
table16 = pd.DataFrame(ProgramReferences)
table18 = pd.DataFrame(FoaInformations)
table19 = pd.DataFrame(Organizations)
table10 = pd.DataFrame(IDs)

# Saving the DataFrames to sql
table11.to_sql('awards', engine, if_exists='append', index=False)
table17.to_sql('institutions', engine, if_exists='append', index=False)
table12.to_sql('instruments', engine, if_exists='append', index=False)
table14.to_sql('investigators', engine, if_exists='append', index=False)
table15.to_sql('programme_elements', engine, if_exists='append', index=False)
table13.to_sql('programme_officers', engine, if_exists='append', index=False)
table16.to_sql('programme_references', engine, if_exists='append', index=False)
table18.to_sql('foa_information', engine, if_exists='append', index=False)
table19.to_sql('organisations', engine, if_exists='append', index=False)
table10.to_sql('ids', engine, if_exists='append', index=False)

cur.close()
conn.close()
