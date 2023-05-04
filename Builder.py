from decimal import Decimal
import nltk,os,csv,re,json,zipfile,string
from os.path import exists
import PySimpleGUI as gui
from nltk.stem import WordNetLemmatizer
from sympy import content
from whoosh.fields import Schema, TEXT, ID
from whoosh import index, qparser
from whoosh.qparser import QueryParser
import pandas as pd
from collections import Counter


strPath = os.path.realpath(__file__)
path = os.path.dirname(strPath)

# File e Directory Globali
collection = f"{path}/Collection/"
index_path = f"{collection}index/"
index_path2 = f"{collection}index2/"
archive = f"{collection}archive/"
files = f"{path}/Collection/files/"
files_json = f"{path}/Collection/files_json/"
directory_to_extract_to = f"{collection}"
# Path csv
amazon_path = "amazon_prime_titles.csv"
netflix_path = "netflix_titles.csv"
imdb_path = "movies_initial.csv"
json_path = "movies.json"

# Index unzipping
directory_to_extract_to = f"{collection}"
zip1 = f"{collection}/index.zip"
zip2 = f"{collection}/index2.zip"

RELEVANCE_THRESHOLD = 7

def creazione_index():
    '''
    Funzione per la creazione dei index.
    '''

    # Creazione struttura Index
    schema = Schema(
        title = TEXT(stored=True),
        director = TEXT(stored=True),
        content = TEXT(stored=True),
        cast = TEXT(stored=True),
        genre = TEXT(stored=True),
        year = TEXT(stored=True),
        descr = TEXT(stored=True),
        path = ID(stored=True),
    )

    #Creazione Indice CSV
    index.create_in(index_path, schema)

    #Creazione Indice json
    index.create_in(index_path2, schema)

def builder(L):
    """ 
    1. title
    2. director
    3. cast
    4. year
    5. genre
    6. descr
    """
    for elem in L:
        title = elem[2]
        title=title.translate(str.maketrans('','',string.punctuation))
        director = elem[3]
        cast = elem[4]
        genre = elem[10]
        year = elem[7]
        descr = elem[11]
        filename = files+title+'.txt'
        f = open(filename, 'w', encoding="utf8")
        f.write(f"{title}\n{director}\n{cast}\n{genre}\n{year}\n{descr}")

def builderIMDB(L):
    for elem in L:
        title = elem[1]
        title=title.translate(str.maketrans('','',string.punctuation))
        director = elem[7]
        cast = elem[9]
        genre = elem[5]
        year = elem[2]
        descr = elem[15]
        filename = files+title+'.txt'
        f = open(filename, 'w')
        f.write(f"{title}\n{director}\n{cast}\n{genre}\n{year}\n{descr}")


def builder_json(data):
    for elem in data:
        title = elem['name']
        title=str(title).translate(str.maketrans('','',string.punctuation))
        try:
            director = elem['director']['name']
        except KeyError:
            director = None
        try:
            genre = elem['genre'][0]
        except IndexError:
            genre = None
        try:
            l = elem['cast']
            cast_string = ""
            for i in range(0,len(l)):
                cast_string += l[i]['name'] +","
            cast_string = cast_string[:-1]
            cast = cast_string
        except KeyError:
            cast = None
        descr = elem['summary_text']
        year = elem['year']
        filename = files_json+title+'.txt'
        f = open(filename,'w', encoding="utf8")
        f.write(f"{title}\n{director}\n{cast}\n{genre}\n{year}\n{descr}")


def csvReader(file):
    with open(file, 'r', encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        output = list(reader)
    return output


def json_reader(file):
    with open(file, 'r', encoding="utf8") as jsonfile:
        movies = json.load(jsonfile)
    return movies


def benchmarking(R,A):
    precision=0
    recall=0
    RA = R.intersection(A)
    try:
        precision = len(RA)/len(A)
    except ZeroDivisionError:
            pass
    print(f"Precision --> {round(Decimal(precision*100),2)} % ")


def query(ix1,ix2,raw_query,filtro):
    
        D = {
            "titolo":"title",
            "anno":"year",
            "cast":"cast",
            "all":"content",
            "regista":"director",
            "genere":"genre"
            }

        results = []

        query = QueryParser(D[filtro], ix1.schema).parse(raw_query)
        results += ix1.searcher().search(query, terms=True)
        q1 = ix1.searcher().search(query, terms=True)

        query = QueryParser(D[filtro], ix2.schema).parse(raw_query)
        results += ix2.searcher().search(query, terms=True)
        q2 = ix2.searcher().search(query, terms=True)
        


        result = {"title": [], "year": [], "content": [], "director": [], "genre": [],"cast":[],"descr":[],"score":[]}
        fields = ["title","year","content", "director", "genre","cast","descr","score"]
        count = len(results)
        result_dict = {}

       
        set1=q1.docs()
        set2=q2.docs()
        A = set1.union(set2)

        R = set()
        for hit in results:
            
            R.add(hit.docnum)
            result_dict[hit['title']] = []
            


        for hit in results:
            result_dict[hit['title']].append(hit.score)
        

        min_scores = []
        max_scores = []
        for k,v in result_dict.items():
            if len(result_dict[k]) > 1:
                min_scores.append(min(result_dict[k]))
                max_scores.append(max(result_dict[k]))

        duplicate_result = []
        for hit in results:
            if hit.score in min_scores:
                duplicate_result.append(dict(title=hit['title'],cast=hit['cast'],genre=hit['genre'],score=hit.score))
        for dct in results:
            for field in fields:
                result[field].append(dct.get(field, round(Decimal(dct.score),2)))
            
        df = pd.DataFrame(result)
        


        df_sorted=df.sort_values("score",ascending=False).drop_duplicates(subset=['title'])

        df_dict = df_sorted.to_dict(orient="list")
        
        tot_score = 0

        if len(df_sorted) == len(results):
            count = len(results)
        else:
            count=len(df_sorted)
        
        if len(duplicate_result) > 0:
            for i in range(count):
                for j in range(len(duplicate_result)):
                    if df_dict['title'][i] == duplicate_result[j]['title']:
                        if len(df_dict['cast'][i]) < len(duplicate_result[j]['cast']):
                            df_dict['cast'][i] = duplicate_result[j]['cast']
                        if len(df_dict['genre'][i]) < len(duplicate_result[j]['genre']):
                            df_dict['genre'][i] = duplicate_result[j]['genre']
                        #df_dict['score'][i] = (df_dict['score'][i] + round(Decimal(duplicate_result[j]['score']),2))/2

        for i in range(count):
            print(df_dict['title'][i])
            print(df_dict['director'][i])
            print(df_dict['cast'][i])
            print(df_dict['year'][i])
            print(df_dict['genre'][i])
            #print(df_dict['descr'][i])
            print("Ranking -->", df_dict['score'][i])
            tot_score+= df_dict['score'][i]
            print("\n-------------------------\n")
        
        benchmarking(R,A)
        
        try:
            print("Ranking medio --> ",round(Decimal(tot_score/count),2))
        except ZeroDivisionError:
            print("no results found")
        
        

def riempimento_index():
        ################# RIEMPIMENTO PRIMO INDICE ###########
    def tokens(text):
        '''
        Funzione per la tokenizzazione
        '''
        tokens = nltk.word_tokenize(text)
        return tokens
    def stopwords(tokens):
        '''
        Funzione per la rimozione delle stopwords.
        '''
        l = []

        from nltk.corpus import stopwords
        wnl = WordNetLemmatizer()
        for t in tokens:
            if not t in stopwords.words('english'):
                if len(t) <= 2:
                    t = t.lower()
                list.append(l, wnl.lemmatize(t))
        return l
    def tagging(tokens):
        '''
        Funzione per il Tagging.
        '''
        return nltk.pos_tag(tokens)
    
    # Caricamento Index File
    writer1 = index.open_dir(index_path).writer()

    for file_name in os.listdir(files):
        
        try:
            file_path = f'{files}{file_name}'
            film_title = ''
            film_dir = ''
            film_cast = ''
            film_genre = ''
            film_year = ''
            film_descr = ''
            film_content = ''
            if file_name.__contains__('.txt'):
                file_text = open(file_path, "r").read().splitlines()
                for word in tagging(stopwords(tokens(file_text[0]))):
                        film_title = film_title + word[0] + " "
                film_content+=film_title
                for word in tagging(stopwords(tokens(file_text[1]))):
                        film_dir = film_dir + word[0] + " " 
                film_content+=film_dir
                for word in tagging(stopwords(tokens(file_text[2]))):
                        film_cast = film_cast + word[0] + " "
                film_content+=film_cast
                for word in tagging(stopwords(tokens(file_text[3]))):
                        film_genre = film_genre + word[0] + " "
                film_content+=film_genre
                for word in tagging(stopwords(tokens(file_text[4]))):
                        film_year = film_year + word[0] + " "
                film_content+=film_year

                for word in tagging(stopwords(tokens(file_text[5]))):
                    if word[1] == "NN" or word[1] == "NNP":
                        film_descr = film_descr + word[0] + " " 
                film_content+=film_descr
    
            else:
                print(f'File non riconosciuto -> {file_name}') 


            writer1.add_document(
                title = film_title,                
                descr = film_descr,
                director = film_dir,
                cast = film_cast,
                genre = film_genre,
                content = film_content,
                year = film_year,
                path = file_path,
            )
        except:
            print(f"Documento non caricato -> {file_name}")

    
    writer1.commit()


    ######### RIEMPIMENTO SECONDO INDEX #####
    
    # Caricamento Index File
    writer2 = index.open_dir(index_path2).writer()

    for file_name in os.listdir(files_json):
        
        try:
            file_path = f'{files_json}{file_name}'
            film_title = ''
            film_dir = ''
            film_genre = ''
            film_year = ''
            film_content = ''
            film_descr = ''
            film_cast=''
            if file_name.__contains__('.txt'):
                file_text = open(file_path, "r").read().splitlines()
                for word in tagging(stopwords(tokens(file_text[0]))):
                        film_title = film_title + word[0] + " "
                film_content+=film_title
                for word in tagging(stopwords(tokens(file_text[1]))):
                        film_dir = film_dir + word[0] + " " 
                film_content+=film_dir
                for word in tagging(stopwords(tokens(file_text[2]))):
                        film_cast = film_cast + word[0] + " "
                film_content+=film_cast
                for word in tagging(stopwords(tokens(file_text[3]))):
                        film_genre = film_genre + word[0] + " "
                film_content+=film_genre
                for word in tagging(stopwords(tokens(file_text[4]))):
                        film_year = film_year + word[0] + " "
                film_content+=film_year
                for word in tagging(stopwords(tokens(file_text[5]))):
                    if word[1] == "NN" or word[1] == "NNP":
                        film_descr = film_descr + word[0] + " " 
                film_content+=film_descr

        
            else:
                print(f'File non riconosciuto -> {file_name}') 


            writer2.add_document(
                title = film_title,                
                descr = film_descr,
                director = film_dir,
                cast = film_cast,
                genre = film_genre,
                content = film_content,
                year = film_year,
                path = file_path
            )
        except:
            print(f"Documento non caricato -> {file_name}")

    
    writer2.commit()


def GUI():
    gui.theme('Black')
    #Creazione Layout GUI
    layout = [
        [
            gui.Text('Immettere query',font=('Roboto',10,'bold')),
            gui.Input(size=(50,1), focus=True, key="QUERY", do_not_clear=False),
            gui.Button('Search', size=(10,1), bind_return_key=True, key="BUTTON_SEARCH"),
            gui.Combo(['titolo','anno','regista','genere','cast','all'],size=(15,1),default_value="all", key='FILTER'),

        ],
        [
            gui.Output(size=(100,30), key="_output_", font=('Roboto',10,'bold'))
        ],
    ]

    window = gui.Window('Search Engine', layout, element_justification='c')
    
    ix1 = index.open_dir(index_path)
    ix2 = index.open_dir(index_path2)

    # Eventi GUI
    
    while True: 
        event, values = window.read()
        if (event == 'BUTTON_SEARCH' or 'INDEX_UPDATER') and values['QUERY'] != '':
            window.find_element('_output_').Update('')
        if event == 'BUTTON_SEARCH' and values['QUERY'] != '':
            window.find_element('_output_').Update('')
            
        if event == 'BUTTON_SEARCH' and values['QUERY'] != '':  
            if(values['FILTER'] == "Filtra per.."):
                values['FILTER'] = "content" 
            query(ix1,ix2, values['QUERY'],values['FILTER'])


        if event == gui.WIN_CLOSED:
            break
    
    window.close()
def unzip(path_to_zip_file):   
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)
if __name__ == "__main__":
    
    # ix = index.open_dir(index_path)
    # ix2 = index.open_dir(index_path2)
    
    
    # query(ix,ix2,"Love movies","all")

     ################# BUILDERS ##############
    # netflix = csvReader(archive+netflix_path) #8807 tuple
    # netflix.pop(0)
    # builder(netflix)

    #movies_json = json_reader(archive+json_path)
    #builder_json(movies_json[0:100000])

    ################# CREAZIONE INDICE ##############
    #creazione_index()
    #riempimento_index()
    
    if not exists(f"{collection}index"):
        unzip(zip1)
    if not exists(f"{collection}index2"):
        unzip(zip2)

    if exists(f"{collection}index") and exists(f"{collection}index2"):
        GUI()

