import requests
from bs4 import BeautifulSoup 
import pandas as pd
from selenium import webdriver  #to handle news source's dynamic website
import datetime
import time
from google_trans_new import google_translator 
from statistics import mean
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import quandl
import sys
import xlwt
import openpyxl

class MT_Sentiment_Analyser:
    ''' CAN INCREASE SCROLL TIME THUS NUMBER OF ARTICLES : m = MT_Sentiment_Analyser(['सेंसेक्स'] ,scroll = 10)
    Scrapes hindi News websites to extract information 
    of a particular index we are interested in'''
    def __init__(self,keywords,scroll = 10):
        self.home_link = 'https://www.bhaskar.com/business/'
        #self.markup = requests.get(self.home_link).text
        self.keywords = keywords
        self.stime = time.time()
        self.scroll = scroll
        print("Initialized home link and keywords in --- %s seconds ---\n" % (time.time() - self.stime))
    def sentiment_score_calculator(self,hin_eng_df):
        print("Begun Sentiment score calculation on --- %s th second ---\n" % (time.time() - self.stime))
        t_art_score = []
        t_hl_score = []
        vader = SentimentIntensityAnalyzer()
        for index, row in enumerate (hin_eng_df.values):
            t_para_score = []
            score = 0
            date = hin_eng_df.index[index]
            hl , art , t_art,t_hl = row
            for para in t_art:
                score = vader.polarity_scores(para)['compound']
                t_para_score.append(score)
            t_hl_score.append(vader.polarity_scores(t_hl)['compound'])
            t_art_score.append(mean(t_para_score))
            #f = lambda x:vader.polarity_scores(x)['compound']
        hin_eng_df['para_avg_Senti_Score'] = t_art_score
        hin_eng_df['headline_Senti_Score'] = t_hl_score
        data = quandl.get("BSE/SENSEX", authtoken="1SnsWfT7hPSiUcZumsa1",start_date = hin_eng_df.index.date[-1],end_date = hin_eng_df.index.date[0])
        data['sensex_open_to_close_price'] = ((data['Close'] - data['Open'])/data['Open'] )*100
        hin_eng_df.to_excel('SentimentScoreForSensexV2.xlsx', sheet_name='Sheet1', index=True, encoding=None)
        data.to_excel('Sensex_dataV2.xlsx', sheet_name='Sheet1', index=True, encoding=None)
        print("\n2 : xls file is successfully created! named : SentimentScoreForSensexV2.xls , Sensex_dataV2.xls")
        print(hin_eng_df)
        
        
    def translator_hack(self,data_fr):
        '''Divides the original dataframe(data_fr) into smaller chunks of dataframe with predefined number of articles
        ,Passes each chunk to translator() func,then Concats chunks into one translated dataframe '''
        print("Begun Translation on --- %s th second ---\n" % (time.time() - self.stime))
        art_per_chunk = 10
        chunks = [data_fr[i:i+art_per_chunk] for i in range(0,data_fr.shape[0],art_per_chunk)] #Chunker_by_list_comprehension
        translated_chunks = []
        for i,chunk in enumerate(chunks):
            try:
                translated_chunks.append(self.translator(chunk))
            except Exception as e:
                print('Error has occured,{} this exception is handled\nProgram Continues...\n'.format(e))
                #translated_chunks.append(translator(chunk , proxies={'http':'209.127.191.180:9279'}))
            sys.stdout.write('\rChunk Processed: {}/{} ...{} sec'.format(i+1,len(chunks),(time.time() - self.stime)))
            sys.stdout.flush()
        trans_df = pd.concat(translated_chunks)
        hin_eng_df = data_fr.merge(trans_df,how = 'inner',left_index=True,right_index=True)
        self.sentiment_score_calculator(hin_eng_df)

    def translator(self,df_section):
        '''INPUT: Untranslated Dataframe
           OUTPUT: Translated Dataframe

           issue: Has a inbuilt timeout limit; temp solution: Try again in an hour; '''
        #translate_text = translator.translate('this great world',lang_tgt='bn')  
        translator = google_translator(url_suffix=['translate.google.com','translate.google.co.in'],timeout=15,proxies={'http':'209.127.191.180:9279'})
        saved_translated_articles = []
        saved_translated_headlines = []
        dates = []
        for i, row in enumerate(df_section.values):
            translated_article = []
            date = df_section.index[i]
            hl,art = row
            for para in art:
                translated_article.append(translator.translate(para))
                #time.sleep(2)
            saved_translated_headlines.append(translator.translate(hl))
            saved_translated_articles.append(translated_article)
            dates.append(date)
            sys.stdout.write('\rTranslated: {}/{} ...{} sec'.format(i+1,len(df_section),(time.time() - self.stime)))
            sys.stdout.flush()
        dic = {'Translated_Articles': saved_translated_articles,'Translated_Headlines': saved_translated_headlines}
        df = pd.DataFrame(dic,index = dates)
        df.index.name = 'Published_date_time'
        print("\nDone! --- %s seconds ---" % (time.time() - self.stime))
        return df
        
    def parse_article(self,links):
        
        '''This function opens individual relevant article through the link provided from the parse() function below 
        and uses beautiful soup library to extract the article content and their published dates'''
        print("Begun extracting each article from fitered links --- %s seconds ---" % (time.time() - self.stime))
        self.saved_articles = []
        self.saved_article_dates =[]
        for link in links:#saved_requestable_links:
            article = []
            article_content = requests.get(link).content
            article_soup = BeautifulSoup(article_content,'html.parser')
            paras = article_soup.findAll("p",{'style':"word-break:break-word"})
            dateandtime = article_soup.find("meta", {"property": "article:published_time"}).attrs['content']
            dateandtime = dateandtime[:-6]
            for para in paras:
                #article = ''.join(para.get_text())
                article.append(para.get_text())
            self.saved_articles.append(article)
            date_time_obj = datetime.datetime.strptime(dateandtime, '%Y-%m-%dT%H:%M:%S')
            self.saved_article_dates.append(date_time_obj)
        dic = {'Headlines':self.saved_links_title,'Articles':self.saved_articles}
        hin_df = pd.DataFrame(dic,index = self.saved_article_dates)
        print("Done! --- %s seconds ---" % (time.time() - self.stime))
        self.translator_hack(hin_df)

    def parse(self):
        
        '''This function opens the website scrolls down for 100 seconds then takes the page source code 
        to traverse and extract news Headlines and Executable Links of relevant articles using keywords,
        Then calls the above function parse_article() with executable link as a parameter'''
        print("Begun Parsing and filtering links with keyword --- %s seconds ---" % (time.time() - self.stime))
        driver = webdriver.Chrome('C:\Program Files\Google\Chrome\Application\chromedriver')
        #url = 'https://www.bhaskar.com/business/'
        driver.get(self.home_link)
        time.sleep(10)
        prev_height = driver.execute_script('return document.body.scrollHeight;')
        limit = 0
        while limit < self.scroll:
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(4)
            new_height = driver.execute_script('return document.body.scrollHeight;')
            #if new_height == prev_height:
            #    break
            prev_height = new_height
            limit += 1
        markup = driver.page_source
        soup = BeautifulSoup(markup,'html.parser')
        links = driver.execute_script
        links = soup.findAll("li",{"class" : '_24e83f49 e54ee612'})
        self.saved_links = []
        self.saved_links_title =[]
        self.saved_requestable_links = []
        for link in links:
            for keyword in self.keywords:
                if keyword in link.text:
                    if link not in self.saved_links: #this condition stops duplicate links
                        self.saved_links.append(link)
                        self.saved_links_title.append(link.text)
                        self.saved_requestable_links.append(str(self.home_link) + str(link('a')[0]['href']))
        print("Done! --- %s seconds ---" % (time.time() - self.stime))
        print('{} articles to be passed for scraping'.format(len(self.saved_requestable_links)))
        self.parse_article(self.saved_requestable_links)


m = MT_Sentiment_Analyser(['सेंसेक्स'],scroll = 50)#'निफ्टी','टाटा स्टील','यस बैंक','5G'])#'बैंकिंग','टाटा डिजिटल',
m.parse()
