import pandas as pd
import pysrt
import re
import numpy as np
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import textstat


def sub_processing(upload_file, df_words, df_idioms):


    # читаем файл субтитров


    try:
        subs = pysrt.from_string(upload_file.getvalue().decode('cp1252'))
        print('Decode ANSI success')
        if subs.text == '':
            subs = pysrt.from_string(upload_file.getvalue().decode('utf-16'))
            print('Decode UTF-16 success')
        # Время начала фильма
        film_start = subs[0].start.hours * 3600 + subs[0].start.minutes * 60 + subs[0].start.seconds
        print('Read file success')
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        return

    # Время окончания фильма
    if subs[-1].index - subs[-2].index < 2:
        film_end = subs[-1].end.hours *3600 + subs[-1].end.minutes *60 + subs[-1].end.seconds
    else:
        film_end = subs[-2].end.hours *3600 + subs[-2].end.minutes *60 + subs[-2].end.seconds
    # Продолжительность фильма
    film_start, film_end = min(film_start, film_end), max(film_start, film_end)
    film_lenght = film_end - film_start

    # Очистка текста
    # text = re.sub('<i>|</i>', '', subs.text)
    text = re.sub('\<.*?\>', '', subs.text)      # удаляем то что в скобках <>
    text = re.sub('\n', ' ', text)               # удаляем разделители строк
    # text = re.sub('<font.*?font>', '', text)
    text = re.sub('\(.*?\)', '', text)           # удаляем то что в скобках ()
    text = re.sub('\[.*?\]', '', text)           # удаляем то что в скобках []
    text = re.sub('[A-Z]+?:', '', text)          # удаляем слова написанные заглавными буквами с двоеточием(это имена тех кто говорит)
    text = re.sub('\.+?:', '\.', text)           # Заменяем троеточия на одну точку
    text = text.lower()
    text = re.sub('[^a-z\.\!\?]', ' ', text)     # удаляем всё что не буквы и не .?!
    text = re.sub(' +', ' ', text)               # удаляем " +"
    # text = re.sub(r'\n ', '', text)
    # Количество предложений
    num_sentence = len(re.split('[\.\?\!]', text))
    # морфологический разбор
    morphs = [_[1] for _ in nltk.pos_tag(re.sub('\n', ' ', text).lower().split(' '))]

    gerund = morphs.count('VBG')

    text_no_preproc = text
    text = re.sub('[^a-z]', ' ', text)     # удаляем всё что не буквы

    # ищем идиомы
    num_idioms = 0
    for idiom in df_idioms['lem_idiom']:
        match = re.finditer(fr'{idiom}', text)
        num_idioms += len([_ for _ in match])

    # Количество символов
    text_len = len(text)
    # Букв в секунду
    sumb_persecond = text_len / film_lenght


    # Избавимся от стоп-слов
    # for stop_word in nltk_stopwords.words('english'):
    #     text = re.sub(f' {stop_word} ', ' ', text)

    # удалим однобуквенные слова
    text = re.sub(' [a-z] ', ' ', text)
    text = re.sub(' [a-z] ', ' ', text)
    text = re.sub(' [a-z] ', ' ', text)

    text_no_lem = text

    # Список уникальных слов
    words = text.split(' ')
    words_unique = []
    for word in words:
        if word not in words_unique:
            words_unique.append(word)

    sumb_perword = (text_len - len(words)) / len(words)
    # words = words_unique

    # Расчитаем количество слов разной сложности в фильме
    difficulty = {'A1' :0,
                  'A2' :0,
                  'B1' :0,
                  'B2' :0,
                  'C1' :0,
                  }

    for word in words_unique:
        match = df_words[df_words['word'] == word]['diff'].values
        if len(match) > 0:
            for dif in match:
                difficulty[dif] += 1

    # Лемматизация
    wnl = WordNetLemmatizer()
    text = ' '.join([wnl.lemmatize(word, wordnet.VERB) for word in text.split(' ')])

    # Посчитаем продолжительность всех фраз
    phrases_lenght = 0
    for sub in subs:
        # Время начала фразы
        phrase_start = sub.start.hours *3600 + sub.start.minutes *60 + sub.start.seconds
        # Время окончания фразы
        phrase_end = sub.end.hours *3600 + sub.end.minutes *60 + sub.end.seconds
        # Продолжительность фразы
        phrases_lenght += max(0, phrase_end - phrase_start)

    sumb_persecond_frases = text_len / phrases_lenght

    A1 = difficulty['A1' ] /len(words_unique)
    A2 = difficulty['A2' ] /len(words_unique)
    B1 = difficulty['B1' ] /len(words_unique)
    B2 = difficulty['B2' ] /len(words_unique)
    C1 = difficulty['C1' ] /len(words_unique)


    avg_dificulty = np.mean([A1, A2 * 1.5, B1 * 5, B2 * 50, C1 * 500])

    # Возвращаем результаты
    return pd.DataFrame({
            'phrases_lenght': [phrases_lenght],
            'A2': [A2],
            'coleman_liau_index':[textstat.coleman_liau_index(text_no_preproc)],
            'word_persentence':[len(words) / max(num_sentence, len(subs))],
            'gulpease_index':[textstat.gulpease_index(text_no_preproc)],
            'gerund_persentence':[gerund / num_sentence],
            'words_unique_perphrase':[len(words_unique) / len(subs)],
            'words_unique_persecond':[len(words_unique) / film_lenght],
            'morphs':[' '.join(morphs)],
            'avg_dificulty':[avg_dificulty],
            'idioms_persentence':[num_idioms / num_sentence],
            'film_lenght':[film_lenght],
            'text_len':[text_len],
            'lematise_text_len':[len(text)],
            'sumb_persecond':[sumb_persecond],
            'sumb_persecond_frases':[sumb_persecond_frases],
            'sumb_perword':[sumb_perword],
            'num_sentence':[max(num_sentence, len(subs))],
            'A1':[A1],
            'B1':[B1],
            'B2':[B2],
            'C1':[C1],
            'phrases_count':[len(subs)],
            'words_count':[len(words)],
            'words_unique_count':[len(words_unique)],
            'lexical_diversity':[len(words_unique) / len(words)],
            'flesch_reading_ease':[textstat.flesch_reading_ease(text_no_preproc)],
            'flesch_kincaid_grade':[textstat.flesch_kincaid_grade(text_no_preproc)],
            'smog_index':[textstat.smog_index(text_no_preproc)],
            'automated_readability_index':[textstat.automated_readability_index(text_no_preproc)],
            'dale_chall_readability_score':[textstat.dale_chall_readability_score(text_no_preproc)],
            'difficult_words':[textstat.difficult_words(text_no_preproc)],
            'linsear_write_formula':[textstat.linsear_write_formula(text_no_preproc)],
            'gunning_fog':[textstat.gunning_fog(text_no_preproc)],
            'text_standard':[textstat.text_standard(text_no_preproc)],
            'fernandez_huerta':[textstat.fernandez_huerta(text_no_preproc)],
            'szigriszt_pazos':[textstat.szigriszt_pazos(text_no_preproc)],
            'gutierrez_polini':[textstat.gutierrez_polini(text_no_preproc)],
            'crawford':[textstat.crawford(text_no_preproc)],
            'osman':[textstat.osman(text_no_preproc)],
            'num_idioms':[num_idioms],
            'gerund':[gerund],
            'text':[text],
            'text_no_lem':[text_no_lem],
    }
    )