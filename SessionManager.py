import requests
from random import randint


class Session_Manager:
    def __init__(self,Proxies_list,Headers_list):
        self.value=0
        self.random_int=0
        self.New_RandomInt()
        self.proxy_object=self.List_Iterator(Proxies_list)
        self.header_object=self.List_Iterator(Headers_list)
        self.session=self.Session_Handler()
        proxy=self.proxy_object.advance()
        print(proxy)
        self.session.New_Session(proxy,self.header_object.advance())

     
                
    def Get(self,url,main_url):
        if self.value == self.randinteger:
            cookie_trap=self.session.Get_html(self.main_url)
            self.New_RandomInt()
            self.session.End_session()
            proxy=self.proxy.advance()
            print(proxy)
            header=self.header.advance()
            self.session.New_Session(proxy,header)
        self.value+=1
        cookie_trap=self.session.Get_html(main_url)
        answer=self.session.Get_html(url)
        return answer
        
    def New_RandomInt(self):
        self.randinteger=randint(15,25)
       

    class Session_Handler:
        def New_Session(self,proxy, headers):
            self.new_session=requests.Session()
            self.new_session.headers.update(headers)
            self.new_session.proxies.update(proxy)

        def Get_html(self,url):
            result=self.new_session.get(url)
            result=result.text
            return result
            
        def End_session(self):
            self.new_session.config['keep_alive']=False


    class List_Iterator:
        def __init__(self,lis):
            self.lis=lis
            self.value=0

        def advance(self):
            try:
                ph=self.lis[self.value]
                self.value+=1
                return ph
            except:
                self.value=0
                ph=self.lis[self.value]
                self.value+=1
                return ph
