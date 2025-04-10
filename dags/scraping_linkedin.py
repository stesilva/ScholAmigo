import warnings
from bs4 import BeautifulSoup
from httpcore import TimeoutException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
import boto3
from datetime import datetime
from send_data_to_aws import send_data_to_aws

warnings.filterwarnings("ignore")
load_dotenv()
options = Options()
options.add_argument('--headless')
options.add_argument("--no-sandbox")
options.binary_location = "/usr/bin/chromium"
service=Service("/usr/bin/chromedriver")

class LinkedInScraper:
    def __init__(self):

        self.driver = webdriver.Chrome(service=service,options=options)
        self.profile_data = {}
        self.base_url = ""

    def scrape_profile(self, url):
        self.base_url = url
        self.driver.get(url)
        self.profile_data['url'] = url

        self.scrape_basic_info()
        self.scrape_section('experience', self.scrape_experiences)
        self.scrape_section('education', self.scrape_educations)
        self.scrape_section('certifications', self.scrape_licenses)
        self.scrape_section('skills', self.scrape_skills)
        self.scrape_section('languages', self.scrape_languages)
        self.scrape_section('projects', self.scrape_projects)
        self.scrape_section('courses', self.scrape_courses)
        self.scrape_section('honors', self.scrape_honors)

    #login to LinkedIn using provided credentials
    def login(self):
        myemail = "bdmproject2025@gmail.com"  # Directly insert your email
        mypassword = "bdmprojecttests"  
        self.driver.get('https://www.linkedin.com/login')
        email = self.driver.find_element(By.ID, 'username')
        email.send_keys(myemail)
        password = self.driver.find_element(By.ID, 'password')
        password.send_keys(mypassword)
        password.submit()


    def scrape_section(self, section_name, scrape_function):
        section_url = f"{self.base_url}/details/{section_name}/" #access directly to the section page
        try:
            print(f"Scraping {section_name}...")
            self.driver.get(section_url)
            self.wait_for_page_load(10) #wait for the page to load
            scrape_function()
        except TimeoutException:
            print(f"Timed out waiting for {section_name} page to load")
        except Exception as e:
            print(f"Error scraping {section_name}: {str(e)}")

    #extract basic profile information
    def scrape_basic_info(self):
        try:
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            self.profile_data['name'] = self.get_text(soup, 'h1', {'class': 'zqDwhjdHBDsxlBkckXblbDrOrRTAPIQDZCds inline t-24 v-align-middle break-words'})
            self.profile_data['headline'] = self.get_text(soup, 'div', {'class': 'text-body-medium'})
        except Exception as e:
            print(f"Error scraping basic info: {str(e)}")

    #for the following sections, the scraping functions follows the same format as Linkedin presentes the data in the same way
    def scrape_experiences(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        print(self.driver.page_source)
        experiences_section = soup.find('div', {'class': 'pvs-list__container'})
        if experiences_section:
            experiences = experiences_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['experiences'] = [self.get_experience(experience) for experience in experiences] #for each experience found
        else:
            print("No experience section found")
            self.profile_data['experiences'] = []

    def scrape_educations(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        educations_section = soup.find('div', {'class': 'pvs-list__container'})
        if educations_section:
            educations = educations_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['educations'] = [self.get_education(education) for education in educations]
        else:
            self.profile_data['educations'] = []

    def scrape_licenses(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        licenses_section = soup.find('div', {'class': 'pvs-list__container'})
        if licenses_section:
            licenses = licenses_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['licenses'] = [self.get_license(license) for license in licenses]
        else:
            self.profile_data['licenses'] = []

    def scrape_skills(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        skills_section = soup.find('div', {'class': 'pvs-list__container'})
        if skills_section:
            skills = skills_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['skills'] = [self.get_skills(skill) for skill in skills]
        else:
            self.profile_data['skills'] = []

    def scrape_languages(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        languages_section = soup.find('div', {'class': 'pvs-list__container'})
        if languages_section:
            languages = languages_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['languages'] = [self.get_languages(language) for language in languages]
        else:
            self.profile_data['languages'] = []        

    def scrape_projects(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        projects_section = soup.find('div', {'class': 'pvs-list__container'})
        if projects_section:
            projects = projects_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['projects'] = [self.get_projects(project) for project in projects]
        else:
            self.profile_data['projects'] = []              

    def scrape_courses(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        courses_section = soup.find('div', {'class': 'pvs-list__container'})
        if courses_section:
            courses = courses_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['courses'] = [self.get_courses(course) for course in courses]
        else:
            self.profile_data['courses'] = []  

    def scrape_honors(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        honors_section = soup.find('div', {'class': 'pvs-list__container'})
        if honors_section:
            honors= honors_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['honors'] = [self.get_honors(honor) for honor in honors]
        else:
            self.profile_data['honors'] = [] 
 
    #handle different layout of the experience section (different roles within one company)
    def get_experience(self, experience):
        experience_dict = {
            'company_name': None,
            'duration': None,
            'designations': []
        }
        company_div = experience.find('div', {'data-view-name': 'profile-component-entity'})
        designation_divs = experience.find_all('li', {'class': 'pvs-list__paged-list-item'})

        if designation_divs:
            experience_dict['company_name'] = self.get_text(company_div, 'span', {'class': 'visually-hidden'})
            experience_dict['duration'] = self.get_nested_text(company_div, 'span', {'class': 't-14 t-normal'}, 'span', {'class': 'visually-hidden'})

            for designation_div in designation_divs:
                designation = {
                    'designation': self.get_text(designation_div, 'span', {'class': 'visually-hidden'}),
                    'duration': None,
                    'location': None
                }
                
                duration_location_spans = designation_div.find_all('span', {'class': 't-14 t-normal t-black--light'})
                if duration_location_spans:
                    designation['duration'] = self.get_nested_text(duration_location_spans[0], 'span', {'class': 'visually-hidden'})
                    if len(duration_location_spans) > 1:
                        designation['location'] = self.get_nested_text(duration_location_spans[1], 'span', {'class': 'visually-hidden'})
                
                experience_dict['designations'].append(designation)
        else:
            designation_name_spans = company_div.find_all('span', {'class': 'visually-hidden'})  if self.verify_content(company_div.find('span', {'class': 'visually-hidden'})) is not None else None
            if designation_name_spans:
                experience_dict['company_name'] = designation_name_spans[1].text.strip() if len(designation_name_spans) > 1 else None
                experience_dict['designations'].append({
                    'designation': designation_name_spans[0].text.strip(),
                    'duration': None,
                    'location': None
                })

            duration_location_spans = company_div.find_all('span', {'class': 't-14 t-normal t-black--light'})
            if duration_location_spans:
                duration = self.get_nested_text(duration_location_spans[0], 'span', {'class': 'visually-hidden'})
                experience_dict['duration'] = duration
                experience_dict['designations'][0]['duration'] = duration
                if len(duration_location_spans) > 1:
                    experience_dict['designations'][0]['location'] = self.get_nested_text(duration_location_spans[1], 'span', {'class': 'visually-hidden'})

        return experience_dict

    #for the following sections, the scraping functions are similar bc the data layout does not vary that much. Only the key attributes were retrieved
    def get_education(self, education):
        if self.verify_content(education.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        education_data = {
            'college': None,
            'degree': None,
            'duration': None
        }

        spans = education.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'college',
            1: 'degree',
            2: 'duration'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                education_data[field_mapping[index]] = span.get_text().strip()

        return education_data

    def get_license(self, license_item):
        if self.verify_content(license_item.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        license_data = {
            'name': None,
            'institute': None,
            'issued_date': None
        }

        spans = license_item.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'name',
            1: 'institute',
            2: 'issued_date'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                if index == 2:
                    license_data[field_mapping[index]] = span.get_text().strip().replace("Issued ", "").replace("Verificação emitida em ", "")
                else:
                    license_data[field_mapping[index]] = span.get_text().strip()


        return license_data
    
    
    def get_skills(self, skill_item):
        if self.verify_content(skill_item.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        skills_data = {
            'name': None
        }

        spans = skill_item.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'name'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                skills_data[field_mapping[index]] = span.get_text().strip()


        return skills_data
    
    def get_languages(self, language_item):
        if self.verify_content(language_item.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        languages_data = {
            'name': None,
            'level': None,
        }

        spans = language_item.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'name',
            1: 'level'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                languages_data[field_mapping[index]] = span.get_text().strip()


        return languages_data
    
    def get_projects(self, project_item):
        if self.verify_content(project_item.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        projects_data = {
            'project_name': None,
            'duration': None,
            'description': None
        }

        spans =  project_item.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'project_name',
            1: 'duration',
            2: 'description'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                projects_data[field_mapping[index]] = span.get_text().strip()


        return projects_data
    
    def get_courses(self, course_item):
        if self.verify_content(course_item.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        courses_data = {
            'course_name': None,
            'associated_with': None
        }

        spans =  course_item.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'course_name',
            1: 'associated_with'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                courses_data[field_mapping[index]] = span.get_text().strip()


        return courses_data
    
    def get_honors(self, honor_item):
        if self.verify_content(honor_item.find('span', {'class': 'visually-hidden'})) is None:
            return {}

        honors_data = {
            'honor_name': None,
        }

        spans =  honor_item.find_all('span', {'class': 'visually-hidden'})
        
        field_mapping = {
            0: 'honor_name'
        }

        for index, span in enumerate(spans):
            if index in field_mapping:
                honors_data[field_mapping[index]] = span.get_text().strip()


        return honors_data
    
    def wait_for_page_load(self, timeout=10):
        try:
            #wait for the body tag to pe loaded
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            print("Page load timed out")

    #get the text from a specific tag and attributes
    @staticmethod
    def get_text(soup, tag, attrs):
        element = soup.find(tag, attrs)
        return element.get_text().strip() if element else None 
    
    #get the text from a nested tag and attributes
    def get_nested_text(self, parent, outer_tag, outer_attrs, inner_tag=None, inner_attrs=None):
        outer_element = parent.find(outer_tag, outer_attrs)
        if not outer_element:
            return None
        if inner_tag and inner_attrs:
            inner_element = outer_element.find(inner_tag, inner_attrs)
            return inner_element.text.strip() if inner_element else None
        return outer_element.text.strip()

    #verify if the content is empty or contains a specific message (no section on Linkedin)
    def verify_content(self, span):
        if not span:
            return None 
        
        text = span.get_text().strip()  
        if text in ['Nada para ver por enquanto', 'Nothing to see yet']:
            return None 
        
        return text  

    def decode_unicode_escape(self,input):
        if isinstance(input, str):
            return input.encode('utf-8').decode('unicode-escape')
        elif isinstance(input, list):
            return [self.decode_unicode_escape(item) for item in input]
        elif isinstance(input, dict):
            return {key: self.decode_unicode_escape(value) for key, value in input.items()}
        else:
            return input
        
    #save the scraped data to S3 bucket
    def save_data(self, bucket_name, all_profile_data, folder_name):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        s3_file_name = f"{folder_name}{timestamp}_linkedin_profile_data.json"
        send_data_to_aws(all_profile_data, bucket_name, s3_file_name)

    def close(self):
        self.driver.quit()        

#retrieve the list of user to perform the scraping
def retrive_linkedin_urls_s3(s3,bucket_name,folder_name):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name) 
    
    if 'Contents' not in response:
        return None
    
    latest_file = max(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_file_key = latest_file['Key']
    
    file_response = s3.get_object(Bucket=bucket_name, Key=latest_file_key)
    content = file_response['Body'].read().decode('utf-8')
    
    return content

def scrape_linkedin_profiles(scraper, linkedin_urls, bucket_name, folder_name):
    all_profile_data = [] 

    lines = linkedin_urls.strip().split("\n")
    lines = [line.replace('\r', '') for line in lines]
    headers = lines[0].split(",")
    rows = [dict(zip(headers, line.split(","))) for line in lines[1:]]

    iteration_count=0
    max_iterations=3 #limit the number of profiles to scrape for demo purposes
    try:
        for row in rows: #for each retrieved profile
            if iteration_count >= max_iterations:
                break
            profile_title = row['name']
            profile_url = row['url']
            print(f"Scraping profile: {profile_title} - {profile_url}")

            try:
                profile_data = scraper.scrape_profile(profile_url)
                if profile_data:
                    all_profile_data.append(profile_data)
                    print(f"Successfully scraped data for: {profile_title}")
                else:
                    print(f"Warning: No data scraped for {profile_title} - {profile_url}")

            except Exception as e:
                print(f"Error scraping {profile_title} - {profile_url}: {e}")
            iteration_count += 1

    except FileNotFoundError:
        print(f"Error: No file found at {bucket_name}")
        return
    except Exception as e:
        print(f"An error occurred while reading the data: {e}")
        return

    try:
        scraper.save_data(bucket_name,all_profile_data, folder_name)
        print(f"Scraped data saved to {bucket_name} - {folder_name}")
    except Exception as e:
        print(f"Error saving data to S3: {e}")        


def scrape_linkedin():

    #retieve the list of users to scrape from S3 bucket
    session = boto3.Session(profile_name="bdm_group_member")
    s3 = session.client("s3")
    bucket_name = 'linkedin-data-bdm'
    input_folder_name = 'application_data/'
    output_folder_name = 'linkedin_users_data/'
    linkedin_urls = retrive_linkedin_urls_s3(s3,bucket_name,input_folder_name)

    scraper = LinkedInScraper()

    scraper.login()
    scrape_linkedin_profiles(scraper,linkedin_urls,bucket_name,output_folder_name)
    scraper.close()
