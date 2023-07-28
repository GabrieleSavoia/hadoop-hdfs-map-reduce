from mrjob.job import MRJob

# Non utilizzo NLTK dal momento che si tratta di codice di esempio
stop_words = ['il', 'con', 'i', 'a', 'e', 'al', 'con', '.', ',', 'nella', 'nei', 'nel', 'per', 'di', 'la', 'va', 'alle']

class InvertedIndexMR(MRJob):
    
    def mapper(self, _, line):
        """
        Input : righe del file.
        Return : coppie (word, doc_id)
        """
        doc_id, doc_text = line.split(':')
        doc_id = doc_id.strip()
        for word in doc_text.split():
            word = word.lower()
            if word not in stop_words:
                yield word, doc_id

    def reducer(self, word, doc_list): 
        """
        Input : coppie (word, [doc_id_1, doc_id_1, ... , doc_id_n] )
        Return : coppie (word, [doc_id_1, ... , doc_id_n] )            --> senza duplicati di documenti
        """
        unique_doc = list(set(doc_list))
        
        yield word, unique_doc

if __name__ == '__main__':
    InvertedIndexMR.run()
