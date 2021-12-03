import requests
import pymysql
import concurrent.futures
from bs4 import BeautifulSoup
from random import randint,uniform
from time import sleep,time
import re
import ast



headers_list=[
     # Firefox 77 Mac
     {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    # Firefox 77 Windows
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    # Chrome 83 Mac
    {
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.google.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
    },
    # Chrome 83 Windows 
    {
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.google.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }]

    
#-------------------------------------------------------------------------------------#
# line structure = [proxy , header, department, link, initial_link, FromTheStart=1/0] #
#-------------------------------------------------------------------------------------#          

class Line_Manager():
    def __init__(self,headers):
        self.finished="no"
        self.department_data=self.retrieve_data()
        self.headers=headers
        self.proxy_time_manager_object=self.proxy_time_manager()
        self.proxies=[]
        self.dirty_proxies=[]
        self.finished_departments=[]
        self.dept_dict={}
        fresh_proxies=self.proxy_time_manager_object.refresh_ips(self.proxies,self.dirty_proxies)
        self.proxies=fresh_proxies[0]
        self.lines=[]
        self.total_amount_of_pages_lis={}
        self.temp_txt_list=[]
        self.create_lines()
        print(self.lines[6])


    def extract_lines(self):
        self.read_finished_departments()
        #---------------LOOP STARTS HERE--------------------------#
        #fetch links----------------------------------------------#
        while self.finished != "yes":
            self.update_dept_dict()
            print("-----------------------new loop------------------------")
            #-------checks if its over--------------------------------#
            self.over_check()
            #---------------------------------------------------------#
            url_list=self.url_lister()
            return_values=self.multithreader(url_list)
            #-map lines with their results----------------------------#
            return_values=self.map_lines_to_results(return_values)
            #-Process reuslts(finds next page and reviews)-----------------------------#
            for n,i in enumerate(return_values):
                    print("-------------new_page--------------------------")
                    print("current return_value:",i)
                    print("page that was just fetched>",self.lines[n][3],"or",self.lines[n][4])
                    if i:
                        #In case the html can't be fetched because of incorrect "next_page" value
                        if i == 1:
                            print("there was a problem fetching the department ",self.lines[n][2][0])
                            continue
                        response=self.classify_response(str(i))
                        if response != None and len(i.text) > 8000:
                            print(len(i.text))
                            print("one good response")
                            
                            #------gather data------------------------#
                            data=self.gather_data(i,n)
                            # RETURNS= [total_pages,current_page,page,next_page,reviews] #
                            #--check if department is finished----------#
                            if self.update_finished_departments(n,data):
                                continue
                            #---save data if adequate and update txt info------#
                            self.update_temp_txt_list(data,n)
                            #--save line------------------------#
                            self.save_line(n,data[3])
                        else:
                            print("one bad response")
                            #get rid of dirty_ip-------------#
                            self.dirty_ip(n)

            #------maintains previous departments progress-------------------------#
            self.equalize_progress()
            print("self_temp_txt_list>",self.temp_txt_list)
            #-----self.temp_list--->txt-------------------------------------------#
            self.temp_txt_list_to_txt()
            self.clear_temp_txt_list()
            print("self_temp_txt_list>",self.temp_txt_list)
            #saves proxy_headers pairs--------------------------------#
            self.save_proxy_header_pairs()

            #-----self.finished departments-->txt----------------------#
            self.finished_departments_to_txt()
            #check the time and updates lines-------------------------#
            alert=self.proxy_time_manager_object.check_the_time()
            if alert:
                self.general_update()
            #sleep-------------#
            print('sleeping...........................................')
            sleep(randint(12,20))    
        #-----LOOP ENDS HERE-----------------------------------------------#



    def create_lines(self):
        "=====================CREATING LINES==========================="
        #import proxy_headers-------------------#
        with open('proxy-header.txt','r') as info:
            ph_lis=info.read().split("^")
        print(ph_lis)
        ph_dict={}
        for i in range(0,(len(ph_lis)-1),2):
            
            ph_dict[ph_lis[i]]=ast.literal_eval(ph_lis[i+1])
        print(ph_dict)
        #---------------------------------------#
        for n,i in enumerate(self.proxies):
            #if department i would use is already done with:------#
            if self.department_data[0][n] in self.finished_departments:
                ph_v=n
                try:
                    while self.department_data[0][ph_v] in self.finished_departments:
                        ph_v+=1
                except Exception as a:
                    print("Exception 'a' at 'create_lines'------->",a)
                    self.finished='yes'
                #---pair proxies with their saved header pairs--------#
                try:
                    #ph_dict[i]= header / self.department_data[0]=deparment
                    self.lines.append([{'http':'http//'+str(i)},ph_dict["{'http': 'http//"+str(i)+"'}"],self.department_data[0][ph_v],self.department_data[1][ph_v],self.department_data[1][ph_v],1])
                #---if pair ins't found >>----------------------------#
                except:
                    self.lines.append([{'http':'http//'+str(i)},self.headers[randint(0,len(headers_list)-1)],self.department_data[0][ph_v],self.department_data[1][ph_v],self.department_data[1][ph_v],1])                    
            else:
                #---pair proxies with their saved header pairs--------#
                try:
                    self.lines.append([{'http':'http//'+str(i)}, ph_dict["{'http': 'http//"+str(i)+"'}"],self.department_data[0][n],self.department_data[1][n],self.department_data[1][n],1])
                    print("proxy paired with header")
                #---if pair ins't found >>----------------------------#
                except:
                    print("Couldn't find proxy header pair at 'create_lines'. Creating Random Pair")
                    self.lines.append([{'http':'http//'+str(i)},self.headers[randint(0,len(headers_list)-1)],self.department_data[0][n],self.department_data[1][n],self.department_data[1][n],1])
                                      

    def general_update(self):
        fresh_proxies=self.proxy_time_manager_object.refresh_ips(self.proxies,self.dirty_proxies)
        self.proxies=fresh_proxies[0]
        print("self.proxies after update: ",self.proxies[5])
        self.update_lines(fresh_proxies[1])
        print("self.lines after update: ",self.lines[5])


    def update_lines(self,new_proxies):
        print("=====================UPDATING LINES===========================")
        for n,d in enumerate(self.lines):
            #if current department is finished-------------------#
            if d[2][0] in self.finished_departments:
                p=0
                try:
                    print("trying to add new department to a line whose department was finished") 
                    while self.department_data[0][n] in self.finished_departments and self.department_data[0][n] in self.lines:
                        p+=1
                    self.lines[n][2]=self.department_data[0][p]
                    self.lines[n][4]=self.department_data[1][p]
                    self.lines[n][3]=self.department_data[1][p]
                    self.lines[n][5]=1
                except IndexError as e :
                    print("IndexError at 'update_lines'--------->",e)
       
        for n,i in enumerate(self.lines):
            if len(i) == 5:
                print("dirty ip being recycled/updated")
                try:
                    self.lines[n].insert(0,{'http':'http//'+str(new_proxies[0])})
                    self.lines[n][5]= 1
                    self.lines[n][3]=self.lines[n][4]
                    new_proxies.remove(new_proxies[0])
                    print("Line size after updating with new proxy> ",len(self.lines[n])," first value> ",self.lines[n][0],"last value>",self.lines[n][5])
                except Exception as e:
                    print("exception at 'update_lines'>> ",e)
                    return 

        
    
    #lines.txt format--> {department:link_to_be_scraped}
    def update_dept_dict(self):
         with open("lines.txt","r") as txt:
            ph_txt=txt.read().split('^')
         for i in range(0,(len(ph_txt)-1),2):
            self.dept_dict[ph_txt[i]]=ph_txt[i+1]

    def read_finished_departments(self):
        with open("finished_departments.txt","r") as txt:
            self.finished_departments=txt.read().split("^")
            #important so there are only words
            self.finished_departments=[i for i in self.finished_departments if i != ""]


    def update_finished_departments(self,number,data):
        #some amazon deparments make the 'last_page' info disappear. This code is to catch that
        if data[0]==None and data[3]!= None and data[4]!=None:
            return 0
        print("total_amount> ",self.total_amount_of_pages_lis[self.lines[number][2][0]])
        if int(data[2])==int(self.total_amount_of_pages_lis[self.lines[number][2][0]]):
            self.finished_departments.append(self.lines[number][2][0])
            print("Department ",self.lines[number][2][0]," is finished")
            return 1
        #some amazon departments end before they 'actually' say they end. Following code finds such departments
        elif data[0] == None and data[3]== None and data[4] != None:
            self.finished_departments.append(self.lines[number][2][0])
            print("Department ",self.lines[number][2][0]," is finished")
            return 1
        #if amazon sends page without lower info for some reason, just reload
        elif data[3] == None and data[0]== None and data[4]!= None:
            print("********Can't find next_pages nor total_amount but found some reviews. Will reload********")
            return 1
        else:
            return 0

    def over_check(self):
        if len(self.department_data) == len(self.finished_departments):
                self.finished="yes"

    def url_lister(self):
        working_lines=[i for i in self.lines if len(i)==6 and i[2][0] not in self.finished_departments]
        url_list=self.url_flag_lister(working_lines)
        return url_list

    def multithreader(self,url_list):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            sleep(uniform(0.5,1.5))
            futures = [executor.submit(self.fetch_html,i[0],i[1],i[2]) for i in url_list]
            return_values = [i.result() for i in futures]
        return return_values
        
    def map_lines_to_results(self,return_values):
        dirty=0
        for n,i in enumerate(self.lines):
            if len(i)==5:
                dirty+=1
                return_values.insert(n,None)
            elif i==1:
                return_values.remove(i)
                return_values.insert(n,None)
        #ratio of dirty ips to clean ones
        if dirty:
            percentage=dirty/len(self.lines)*100
                
                
        return return_values

    def equalize_progress(self):
        for i in self.dept_dict:   
            if i not in self.temp_txt_list:
                self.temp_txt_list.append(i)
                self.temp_txt_list.append(self.dept_dict[i])

    def temp_txt_list_to_txt(self):
        with open('lines.txt','w') as l:
            for i in self.temp_txt_list:
                l.write(i+"^")

    def clear_temp_txt_list(self):
        self.temp_txt_list.clear()

    def gather_data(self,response,number):
        print("=====================GATHERING PAGE DATA===========================")
        soup=BeautifulSoup(response.text,'lxml')
        try:                            
            total_pages=self.total_amount_of_pages(soup)
            self.total_amount_of_pages_lis[self.lines[number][2][0]]=total_pages
        except Exception as e:
            print(e)
            total_pages=None
        current_page=self.lines[number][3][0]
        page=self.pagefy(current_page)
        print("current page: ",page)
        try:
            next_page=self.next_page(soup,page)
            reviews=self.fetch_and_add_reviews(soup)
            print("next_page:",next_page)
        except Exception as o:
            print("exception 'o' at 'gather_data'(it can be that the deparment is over): ",o)
            current_page,next_page,reviews=None,None,None
        return[total_pages,current_page,page,next_page,reviews]

    #
    def update_temp_txt_list(self,data,n):
        if len(self.dept_dict)>0:
            try:
                progress=self.pagefy(self.dept_dict[self.lines[n][2][0]])
                if  data[2] > progress:
                    self.save_data(self.lines[n][2][0],data[4])
                    self.temp_txt_list.append(self.lines[n][2][0])
                    self.temp_txt_list.append(self.lines[n][3][0])
                else:
                    return
            except KeyError:
                self.temp_txt_list.append(self.lines[n][2][0])
                self.temp_txt_list.append(self.lines[n][3][0])   
        else:
            self.save_data(self.lines[n][2][0],data[4])
            self.temp_txt_list.append(self.lines[n][2][0])
            self.temp_txt_list.append(self.lines[n][3][0])

    #gets data from the sql server
    def retrieve_data(self):
        connection=pymysql.connect(host="127.0.0.1", user="root",passwd=None, db="mysql")
        cursor=connection.cursor()
        cursor.execute("USE amazon")
        cursor.execute("SELECT links FROM link")
        links=cursor.fetchall()
        cursor.execute("SELECT subcategories FROM link")
        department=cursor.fetchall()
        cursor.close
        connection.close()
        return [department,links]

    def dirty_ip(self,n):
        value=self.lines[n][0]['http']
        dirty=value.replace('https//','')
        self.dirty_proxies.append(dirty)
        self.lines[n].remove(self.lines[n][0])

    
    def save_line(self,number,data):
        self.lines[number][5]=0
        self.lines[number][3]=("https://www.amazon.com"+str(data),)


    def save_proxy_header_pairs(self):
        with open('proxy-header.txt','w') as l:
            for i in self.lines:
                if len(i) ==6:
                    l.write(str(i[0])+"^")
                    l.write(str(i[1])+"^")

    def finished_departments_to_txt(self):
        if len(self.finished_departments) > 0:
            with open('finished_departments.txt','w') as l:
                    for i in self.finished_departments:
                        l.write(i+"^")
                
    def url_flag_lister(self,working_lines):
        url_list=[]
        for i in working_lines:
            if i[5]:
                url_list.append([i[4][0],i[0],i[1]])
                
            else:
                url_list.append([i[3][0],i[0],i[1]])
        return url_list


    def fetch_html(self,current_page_url,proxy,header):
        try:
            response=requests.get(current_page_url,headers=header,proxies=proxy)
            return response
        except Exception as e:
            print("exception at 'fetch_html'`------->",e)
            return 1

  
    def classify_response(self,response):
        response=response.replace('<Response ',"")
        response=response.replace(']>',"")
        response=response.replace('[',"")
        response=int(response)
        if response > 299:
            return None
        else:
            return 1

    def total_amount_of_pages(self,soup):
        
        total_amount=soup.find('li',{'aria-disabled':'true'})
        total_amount=int(total_amount.text)
        return total_amount

    def next_page(self,soup,current_page):
        ph=soup.findAll('li',{'class':'a-normal'})
        ph=str(ph)
        ph=BeautifulSoup(ph,'lxml')
        second_page=ph.find('a',text=re.compile(str(int(current_page)+1)))
        second_page=second_page.attrs
        second_page=second_page['href']
        return second_page

    def fetch_and_add_reviews(self,soup):
        ph_list=[]
        result = soup.findAll(lambda tag: tag.name == 'span' and tag.get('class') == ['a-size-base'])
        for r in result:
            ph=self.string_to_int(r.text)
            ph_list.append(ph)
        total_reviews=0
        for review in ph_list:
            try:
                 total_reviews+=int(review)
            except:
                 pass
        return total_reviews

    def save_data(self,department,review_count):
        print("------------------------saving data--------------------------------")
        connection=pymysql.connect(host="127.0.0.1", user="root",passwd=None, db="mysql")
        cursor=connection.cursor()
        cursor.execute("USE amazon")
        cursor.execute('INSERT INTO reviews(subdepartments,number_of_reviews) VALUES ("'+str(department)+'","'+str(review_count)+'");')
        connection.commit()
        cursor.close
        connection.close()

 
    def pagefy(self,url):
        ph_v=url.find('pg')
        ph_v+=3
        page=url[ph_v:]
        return page

           
    def string_to_int(self,string):
        final_string=string[:string.find(',')]+string[string.find(',')+1:]
        return final_string

                           
    class proxy_time_manager:
        def __init__(self):
            self.first_time=0
            self.ph_time=0
            self.proxies=0

                return None
                

        def refresh_ips(self,self_proxies,dirty_proxies):
            ph_proxies=requests.get('https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=US&ssl=all&anonymity=all&simplified=true')
            formatted_results=ph_proxies.text.split()
            new_proxies=[i for i in formatted_results if i not in self_proxies and i not in dirty_proxies]
            for i in formatted_results:
                if i not in self_proxies:
                    self_proxies.append(i)
            return [self_proxies,new_proxies]






line_manager_object=Line_Manager(headers_list)
a=line_manager_object.extract_lines()













