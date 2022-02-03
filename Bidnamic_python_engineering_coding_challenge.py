# -*- coding: utf-8 -*-
"""
Created on Wed Feb  2 16:45:08 2022

@author: Josep
"""

import psycopg2
import pandas as pd

#give server details
hostname = 'localhost'
database = 'demo'
username = 'postgres'
pwd = 'admin'
port_id = '5432'

conn = None

try:
    #connect to server
    with psycopg2.connect(
        dbname = database,
        user = username,
        password = pwd,
        host = hostname,
        port = port_id ) as conn:
    
        #create cursor
        with conn.cursor() as cur:
            
            #create tables script
            
            createCampaignsScript = ''' CREATE TABLE IF NOT EXISTS campaigns (
                                        campaign_id         INT             PRIMARY_KEY
                                        structure_value     VARCHAR(50) NOT NULL
                                        status              VARCHAR(50) NOT NULL
                                        )
                                    '''
        
            createAdGroupsScript =  ''' CREATE TABLE IF NOT EXISTS ad_groups (
                                        ad_group_id         INT             PRIMARY_KEY
                                        campagn_id          INT
                                        alias               TEXT NOT NULL
                                        status              VARCHAR(50) NOT NULL
                                        )
                                    '''
            createSearchTermsScript = ''' CREATE TABLE IF NOT EXISTS search_terms (
                                        date                DATE
                                        ad_group_id         INT
                                        campagn_id          INT
                                        clicks              INT
                                        cost                NUMERIC(5,2)
                                        conversion_value    NUMERIC
                                        conversions         INT
                                        search_term         TEXT
                                        )
                                    '''
            #create tables
            cur.execute(createCampaignsScript)
            cur.execute(createAdGroupsScript)
            cur.execute(createSearchTermsScript)
            
            #insert records into tables script
            insertCampaignScript = '''INSERT INTO campaign (
                                        campaign_id,
                                        structure_value,
                                        status)
                                        VALUES(%s, %s, %s)
                                    '''
                                    
            insertAdGroupScript = '''INSERT INTO ad_group (
                                        ad_group_id,
                                        campagn_id,
                                        alias,
                                        status)
                                        VALUES(%s, %s, %s, %s)
                                    '''
            insertSearchTermsScript = '''INSERT INTO search_terms (
                                        date,
                                        ad_group_id,
                                        campagn_id,
                                        clicks,
                                        cost,
                                        conversion_value,
                                        conversions,
                                        search_term )
                                        VALUES(DATE %s, %s, %s, %s, %s, %s, %s, %s)
                                    '''
                                    
                                    
            #loading CSVs into pandas data frame
            dfCampaign = pd.read_csv('C:/Users/Josep/Documents/Bidnamic/bidnamic-python-challenge-master/campaigns.csv')
            dfAdGroup = pd.read_csv('C:/Users/Josep/Documents/Bidnamic/bidnamic-python-challenge-master/adgroups.csv')
            dfSearchTerms = pd.read_csv('C:/Users/Josep/Documents/Bidnamic/bidnamic-python-challenge-master/search_terms.csv')
            
            #converting df to list of tuples
            recordsCampaign = dfCampaign.to_records(index=False)
            recordsAdGroup = dfAdGroup.to_records(index=False)
            recordsSearchTerms = dfSearchTerms.to_records(index=False)
            
            #inserting records into tables
            for i in range(len(recordsCampaign)):
                cur.execute(insertCampaignScript, recordsCampaign[i])
                
            for i in range(len(recordsAdGroup)):
                cur.execute(insertAdGroupScript, recordsAdGroup[i])
                
            for i in range(len(recordsSearchTerms)):
                cur.execute(insertSearchTermsScript, recordsSearchTerms[i])
                
            
            #return top 10 search terms for a given camapign structure_value
            structure_value = ''
            
            cur.execute(''' SELECT s.search_terms, s.conversion_value / s.cost AS roas 
                            FROM campaign AS c
                            JOIN search_terms AS s ON c.campaign_id = s.campaign_id
                            WHERE c.structure_value = %s
                            ORDER BY s.conversion_value / s.cost DESC
                            
                        ''', structure_value)
                        
            for records in cur.fetchall():
                print(records)
            
            #return top 10 search terms for a given ad_group alias
            alias = ''
            
            cur.execute(''' SELECT s.search_terms, s.conversion_value / s.cost AS roas 
                            FROM ad_group AS ag
                            JOIN search_terms AS s ON ag.ad_group_id = s.ad_group_id
                            WHERE ag.alias = %s
                            ORDER BY s.conversion_value / s.cost DESC
                            
                        ''', alias)
            
            for records in cur.fetchall():
                print(records)
            
            #NOTE: not sure how to set up a private endpoint for the return queries
            
            
except Exception as error:
    print(error)
finally:
    if conn is not None:
        conn.close()