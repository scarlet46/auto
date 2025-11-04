import pandas as pd
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service  
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
def extract_data_availability(url):
    service=Service(r"/Users/Z/Downloads/chromedriver-mac-arm64/chromedriver")
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--headless')
    web = Chrome(service=service,options=chrome_options)
    web.get(url)  
    # keywords = ["Deposited data", "Gene Expression Omnibus", "Data availability","Data availability statement","availability"]   
    results = []
    
    # check if exits
    try:
        data_anchor_tag = web.find_element(By.XPATH, "//a[@data-ga-label='Full Text Availability']")
        results.append("Page Not Found")
    except NoSuchElementException:
        pass
    
    # Deposited data/Gene Expression Omnibus
    try:
        data_anchor_tag = web.find_element(By.XPATH, "//p[contains(text(), 'Deposited data') or contains(text(), 'Gene Expression Omnibus')]")
        results.append(data_anchor_tag.text)
    except NoSuchElementException:
        pass
    # Data and materials availability
    try:
       data_anchor_tag = web.find_element(By.XPATH, "//*[@id='P65']/strong[text()='Data and materials availability:']")        
       data_paragraph= data_anchor_tag.find_element(By.XPATH, "./..")
       results.append(data_paragraph.text)
    except NoSuchElementException:    
        pass
    try:
        #Data availability
        data_anchor_tag = web.find_element(By.XPATH, "//a[@data-ga-label='Data availability']")
        data_anchor_id = data_anchor_tag.get_attribute('data-anchor-id')
        data_anchor_tag.click()
        content_section = web.find_element(By.CSS_SELECTOR, f"#{data_anchor_id}")
        extracted_content = content_section.text
        extracted_content = extracted_content.replace('\n', ':')
        results.append(extracted_content) 
    except NoSuchElementException:
        pass
    try:
        # Data Availability Statement
        data_anchor_tag = web.find_element(By.XPATH, "//*[@id='_adda93_']//h3[text()='Data Availability Statement']")
        paragraphs = data_anchor_tag.find_elements(By.XPATH, "..//p")
        if paragraphs:
            for paragraph in paragraphs:
                results.append(paragraph.text)
    except NoSuchElementException:
        pass
    # availability
    try:
        data_anchor_tag = web.find_element(By.XPATH, "//p[contains(text(), 'availability')]")
        results.append(data_anchor_tag.text)
    except NoSuchElementException:
        pass
    web.quit()
    return results

def main():
    # pmcid
    data=pd.read_csv('./newest/filtered_result.csv')
    for index, PMCID in enumerate(data['PMCID']):
        if pd.notna(PMCID) and pd.isna(data.loc[index, 'Accession']):
            url="https://www.ncbi.nlm.nih.gov/pmc/articles/"+str(PMCID)
            try:
                extracted_info=extract_data_availability(url)
                extracted_info= ', '.join(extracted_info) if extracted_info else ''
                print(index+1)
                # if isinstance(extracted_info, list):
                #         extracted_info_str = ', '.join(map(str, extracted_info))
                # else:
                #         extracted_info_str = str(extracted_info)
                # data.loc[index, 'Accession-source'] = extracted_info_str
                data.loc[index, 'Accession-source'] = extracted_info
            except Exception :
                data.loc[index, 'Accession-source'] = None
    # #doi
    # for index, DOI in enumerate(data['DOI']):   
    #     if pd.isna(data.loc[index,'PMCID']) and pd.notna(DOI) and pd.isna(data.loc[index, 'Accession']):
    #         url="https://doi.org/"+str(DOI)
    #         try:
    #             extracted_info=extract_data_availability(url)
    #             if isinstance(extracted_info, list):
    #                     extracted_info_str = ', '.join(map(str, extracted_info))
    #             else:
    #                     extracted_info_str = str(extracted_info)
    #             data.loc[index, 'Accession-source'] = extracted_info_str
    #         except Exception as e:
    #             data.loc[index, 'Accession-source'] = None
    
    data.to_csv("./newest/result_accession.csv",index=False)

if __name__ == '__main__':
    print("start get_accession")
    main()
    print("get_accession done")
    
