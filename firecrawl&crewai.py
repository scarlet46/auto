#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 21:06:20 2025

@author: z
"""

###load result[pumid/doi] to Accession-source###

from firecrawl import FirecrawlApp
from crewai import Agent,Task,Crew,LLM

def scrape_data(url, api_key):
    app = FirecrawlApp(api_key)
    scraped_data = app.scrape_url(url)
    return scraped_data

def analyze_with_gpt(full_text, openai_api_key):
    llm = LLM(model="gpt-4o", api_key=openai_api_key)
    analyst = Agent(
        name="Bioinformatics Analyst",
        description="accession source",
        llm=llm,
        verbose=True
    )
    analysis_task = Task(
        description=f"""Analyze the following scientific text and:
        1. Comprehensively identify ALL accession numbers (GSE, GSM, SRP, PRJNA, etc.)
        2. Validate their authenticity based on database standards
        3. Provide context about each identified number
        
        Text Content:
        {full_text[:15000]}  # Truncate for token limits
        """,
        agent=analyst,
        expected_output="Formatted list of accession numbers with validation status and context"
    )
    
    crew = Crew(agents=[analyst], tasks=[analysis_task])
    return crew.kickoff()

def main():
    firecrawl_api = "fc-012fd18b37944149abc8a03d5a08228b"
    # openai_api_key = "" 
    target_url = 'https://pubmed.ncbi.nlm.nih.gov/PMC10412454'
    full_text = scrape_data(target_url, firecrawl_api)
    # analysis_result = analyze_with_gpt(full_text, openai_api_key)
    # print(analysis_result)

if __name__ == "__main__":
    main()
    
    
    