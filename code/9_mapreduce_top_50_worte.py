from mrjob.job import MRJob  # Importieren der MRJob-Klasse aus dem mrjob.job-Modul
from mrjob.step import MRStep  # Importieren der MRStep-Klasse aus dem mrjob.step-Modul
import re  # Importieren des re-Moduls für die Arbeit mit regulären Ausdrücken
"""
json_reader = pd.read_json('yelp_academic_dataset_tip.json', lines=True)


column_data = json_reader['text']  # Spalte auswählen

# Speichern Sie die Spalte in einer zeilenweisen Textdatei
column_data.to_csv('spalte.txt', index=False, header=False, line_terminator='\n')
"""



WORD_RE = re.compile(r"[\w']+")  # Definieren des regulären Ausdrucks zum Finden von Wörtern

# Definieren einer neuen Klasse MRMostUsedWord, die von MRJob erbt
class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,  # Erster Schritt: Mapper, Combiner und Reducer
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),

            MRStep(reducer=self.reducer_50_Top_word)  # Zweiter Schritt: Reducer
        ]

    def mapper_get_words(self, _, line):  # Mapper-Funktion: Extrahiert Wörter aus einer Zeile
        for word in WORD_RE.findall(line):  # Finden aller Wörter in der Zeile mithilfe des regulären Ausdrucks
            word = word.lower()  # Konvertieren des Worts in Kleinbuchstaben
            if len(word) > 3 and not word.isdigit():  # Überprüfen der Bedingungen für ein gültiges Wort
                yield (word, 1)  # Ausgabe des Worts und der Anzahl 1

    def combiner_count_words(self, word, counts):  # Combiner-Funktion: Lokale Aggregation der Wortanzahlen
        yield (word, sum(counts))  # Ausgabe des Worts und der summierten Anzahl

    def reducer_count_words(self, word, counts):  # Reducer-Funktion: Aggregation der Wortanzahlen
        yield None, (sum(counts), word)  # Ausgabe von None als Schlüssel und der summierten Anzahl und dem Wort

    def reducer_50_Top_word(self, _, word_count_pairs):  # Reducer-Funktion: Auswahl der Top 50 Wörter
        top_words = sorted(word_count_pairs, reverse=True)[:50]  # Sortieren der Paare nach der Anzahl in absteigender Reihenfolge und Auswahl der Top 50
        for count, word in top_words:  # Iterieren über die Top 50 Paare
            yield word, count  # Ausgabe des Worts und der Anzahl

if __name__ == '__main__':
    MRMostUsedWord.run()  # Ausführen des MapReduce-Jobs


