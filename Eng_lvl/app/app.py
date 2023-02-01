import streamlit as st
from srt_procesing import sub_processing
from catboost import CatBoostClassifier, Pool
import pandas as pd
import nltk


print('Start app')
df_words = pd.read_csv('Oxford_dikt.csv')
df_idioms = pd.read_csv('theidioms_com.csv', sep='#')
model = CatBoostClassifier()
model.load_model('catboostclassifier_model.cbm')
features = ['phrases_lenght',
            'A2',
            'coleman_liau_index',
            'word_persentence',
            'gulpease_index',
            'gerund_persentence',
            'words_unique_perphrase',
            'words_unique_persecond',
            'morphs',
            'avg_dificulty',
            'idioms_persentence']

st.title('Классификация фильмов по сложности восприятия английского языка')


upload_file = st.file_uploader('Откройте файл субтитов в формате .sts', type='srt')

def make_prdict(data, model):
    """
    :param data:
    :param model:
    :return:
    """
    predict_pool = Pool(data=data,
                        text_features=['morphs']
                       )
    predict = model.predict(predict_pool)
    decode = {2:'A2',
              3:'B1',
              4:'B2',
              5:'C1'
             }

    return decode[predict[0][0]]

if upload_file:

    print(upload_file.name)

    df = sub_processing(upload_file, df_words, df_idioms)
    if df is None:
        st.write('Файл субтитров имеет неизвестный формат')
    else:
        st.header(f'Данный фильм имеет уровень **{make_prdict(df[features], model)}** :sunglasses: по классификации CEFR')

        button = st.button('Показать анализ')
        if button:

            st.header('Содержание слов по сложности согласно Оксфордского словаря')
            st.bar_chart(df.loc[0, ['A1', 'A2', 'B1', 'B2', 'C1']])

            st.write(pd.DataFrame(
                {
                    'Характеристика':['Продолжительность фильма',
                                      'Количество слов',
                                      'Количество уникальных слов',
                                      'Количество фраз',
                                      'Средний темп речи',
                                      'Лексическое разнообразие',
                                     ],
                    'Значение':[f'{df.loc[0,"film_lenght"]//3600} ч. {(df.loc[0,"film_lenght"]%3600)//60} м.',
                                df.loc[0,'words_count'],
                                df.loc[0,'words_unique_count'],
                                df.loc[0,'phrases_count'],
                                f'{round(df.loc[0,"words_count"] / df.loc[0,"film_lenght"] * 60)} сл/мин',
                                round(df.loc[0,'lexical_diversity'], 3),
                               ],
                }
            ))

