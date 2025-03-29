import warnings
import os
import json
import csv
from bs4 import BeautifulSoup
from httpcore import TimeoutException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

class LinkedInScraper:
    def __init__(self):
        self.driver = webdriver.Chrome()
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

    def login(self):
        self.driver.get('https://www.linkedin.com/login')
        email = self.driver.find_element(By.ID, 'username')
        email.send_keys(os.environ['EMAIL'])
        password = self.driver.find_element(By.ID, 'password')
        password.send_keys(os.environ['PASSWORD'])
        password.submit()


    def scrape_section(self, section_name, scrape_function):
        """Generic method to handle section scraping"""
        section_url = f"{self.base_url}/details/{section_name}/"
        try:
            print(f"Scraping {section_name}...")
            self.driver.get(section_url)
            self.wait_for_page_load(10)
            scrape_function()
        except TimeoutException:
            print(f"Timed out waiting for {section_name} page to load")
        except Exception as e:
            print(f"Error scraping {section_name}: {str(e)}")


    def scrape_basic_info(self):
        try:
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            self.profile_data['name'] = self.get_text(soup, 'h1', {'class': 'zqDwhjdHBDsxlBkckXblbDrOrRTAPIQDZCds inline t-24 v-align-middle break-words'})
            self.profile_data['headline'] = self.get_text(soup, 'div', {'class': 'text-body-medium'})
        except Exception as e:
            print(f"Error scraping basic info: {str(e)}")


    def scrape_experiences(self):
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        experiences_section = soup.find('div', {'class': 'pvs-list__container'})
        if experiences_section:
            experiences = experiences_section.find_all('li', {'class': 'pvs-list__paged-list-item artdeco-list__item pvs-list__item--line-separated pvs-list__item--one-column'})
            self.profile_data['experiences'] = [self.get_experience(experience) for experience in experiences]
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
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            WebDriverWait(self.driver, timeout).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, '.artdeco-loader'))
            )
        except TimeoutException:
            print("Page load timed out")

    @staticmethod
    def get_text(soup, tag, attrs):
        element = soup.find(tag, attrs)
        return element.get_text().strip() if element else None 
    
    def get_nested_text(self, parent, outer_tag, outer_attrs, inner_tag=None, inner_attrs=None):
        outer_element = parent.find(outer_tag, outer_attrs)
        if not outer_element:
            return None
        if inner_tag and inner_attrs:
            inner_element = outer_element.find(inner_tag, inner_attrs)
            return inner_element.text.strip() if inner_element else None
        return outer_element.text.strip()

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
        
        
    def save_data(self, filename, all_profile_data):
        with open(filename, 'w') as f:
            json.dump(self.decode_unicode_escape(all_profile_data), f, indent=4, ensure_ascii=False)    

    def close(self):
        self.driver.quit()        
        

def scrape_linkedin_profiles(scraper, csv_file_path, output_json_path):
    all_profile_data = [] 

    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                profile_url = row['url']
                profile_title = row['title'] 
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

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_file_path}")
        return
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")
        return

    try:
        scraper.save_data(output_json_path, all_profile_data)
        print(f"Scraped data saved to {output_json_path}")
    except Exception as e:
        print(f"Error saving data to JSON: {e}")        


def main():
    scraper = LinkedInScraper()
    scraper.login()
    scrape_linkedin_profiles(scraper,'data/normalized_linkedin_data.csv','data/profile_data.json')
    scraper.close()

if __name__ == "__main__":
    main()
