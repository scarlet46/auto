import pandas as pd
from pubmed_mapper import Article
import re
from tqdm import tqdm

def get_article_info(pmid):
    try:
        article = Article.parse_pmid(pmid)
        result = {
            'title_pub': article.title if article.title else None,
            'Authors': re.sub(r'^\[|\]$', '', str(article.authors)) if article.authors else None,
            'Citation': article.references[0].citation if article.references and article.references[0].citation else None,
            'First Author': article.authors[0] if article.authors and len(article.authors) > 0 else None,
            'Journal/Book': article.journal if article.journal else None,
            'PubDate': article.pubdate if article.pubdate else None,
            'Abstract': re.sub(r'<[^>]+>', '', article.abstract) if article.abstract else None
        }
        ids = article.ids if article.ids and len(article.ids) > 1 and article.ids[1].id_value else None
        for id in ids:
            if id.id_type=='pmc':
                result['PMCID'] = id.id_value
            elif id.id_type=='doi':
                result['DOI'] = id.id_value
            elif id.id_type=='mid':
                result['NIHMS ID'] = id.id_value
        return result
    except Exception as e:
        # print(f"Error fetching data for PMID {pmid}: {e}")
        return {
            'title_pub': None,
            'Authors': None,
            'Citation': None,
            'First Author': None,
            'Journal/Book': None,
            'PubDate': None,
            'PMCID':None,
            'NIHMS ID':None,
            'DOI': None,
            'Abstract': None
        }
    
    
# def main():
#     data=pd.read_csv('./newest/result_accession.csv')
#     for index, row in data.iterrows():
#         if pd.notna(row["PubmedID"]) and pd.isna(row["title_pub"]):#PubmedID不为空，title_pub为空
#             data.loc[index,'Url']="https://pubmed.ncbi.nlm.nih.gov/"+str(int(row['PubmedID']))+"/"
#             pmid = row['PubmedID']
#             info = get_article_info(pmid)
#             data.loc[index, info.keys()] = info.values()
#     data.to_csv("./newest/result.csv",index=False)

def main():
    data = pd.read_csv('./newest/result_accession.csv')
    for index, row in tqdm(data.iterrows(), total=data.shape[0], desc='Processing'):
        if pd.notna(row["PubmedID"]) and pd.isna(row["title_pub"]):  # PubmedID不为空，title_pub为空
            data.loc[index, 'Url'] = f"https://pubmed.ncbi.nlm.nih.gov/{int(row['PubmedID'])}/" 
            pmid = row['PubmedID']
            info = get_article_info(pmid)
            data.loc[index, info.keys()] = info.values()
    data.to_csv("./newest/result.csv", index=False)


if __name__ == '__main__':
    print("start get_pubmed")
    main()
    print("get_pubmed done")
    
