# Импорт библиотек
import pandas as pd
import numpy as np

from yargy import Parser, rule
from yargy.pipelines import morph_pipeline
from yargy.predicates import type, normalized
from yargy.tokenizer import MorphTokenizer
from yargy import interpretation
from yargy.interpretation import fact

from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NamesExtractor,
    Doc
)


# Загрузка данных
df = pd.read_csv('test_data.csv')


# Создаём экземпляры
segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
syntax_parser = NewsSyntaxParser(emb)
names_extractor = NamesExtractor(morph_vocab)

NAME = type('RU')


# Парсер приветствия
hello_pipline = morph_pipeline(['здравствуйте', 
                                'привет', 
                                'приветствую', 
                                'добрый день', 
                                'добрый вечер', 
                                'доброе утро', 
                                'доброй ночи'
                              ])
parser_hello = Parser(rule(hello_pipline))


# Парсер прощания
goodbye_pipline = morph_pipeline(['до свидания', 
                                  'до встречи', 
                                  'до завтра', 
                                  'пока'
                                ])
parser_goodbye = Parser(rule(goodbye_pipline))


# ПРЕДСТАВЛЕНИЕ МЕНЕДЖЕРА
# Создаём правило
Intro = fact('Name', ['before', 'between', 'after'])
before  = NAME.interpretation(Intro.before.custom(str))
between = NAME.interpretation(Intro.between.custom(str))
after   = NAME.interpretation(Intro.after.custom(str))

introduce = morph_pipeline(['меня', 'это', 'я'])

intro_rule = rule(before.optional(),
                  introduce,
                  between.optional(),
                  normalized('зовут').optional(), 
                  after.optional(),
).interpretation(Intro)
# Создаём парсер на основе правила
parser_intro = Parser(intro_rule)

# Здесь одним этим парсером не обойтись - необходима дополнительная логика - завернём её в функцию

def manager_name_extractor(text):
    """ Принимаем текст,
        ищем есть ли слова представления,
        Если находим - возвращаем флаг и имя менеджера
    """
    matches_intro = parser_intro.find(text)
    if matches_intro:
        # определяем имя менеджера в позициях по приоритету 
        if matches_intro.fact.between:
            manager_name = matches_intro.fact.between
        elif matches_intro.fact.after:
            manager_name = matches_intro.fact.after
        elif matches_intro.fact.before:
            manager_name = matches_intro.fact.before
        # Проверяем, то что мы нашли действительно имя?
        pobable_name = names_extractor.find(manager_name)
        if pobable_name:
            if pobable_name.fact.first:
                # Если нашли имя - возвращаем флаг и имя
                return True, manager_name
        # Если не нашли - возвращаем флаг - False и имя NaN
    return False, np.nan


# НАЗВАНИЕ КОМПАНИИ
# Создаём правило


Company = fact('company_name', ['first', 'second'])

first  = NAME.interpretation(Company.first.custom(str))
second = NAME.interpretation(Company.second.custom(str))

company = morph_pipeline(['компания', 'фирма', 'организация'])

company_rule = rule(company,
                    first.optional(),
                    second.optional()
).interpretation(Company)
# Создаём парсер на основе правила
parser_company = Parser(company_rule)


def company_name_extractor(text):
    """ Принимаем текст,
        ищем есть ли слова указывающие на название компании,
        Если находим - имя компании
    """
    company_name = np.nan
    matches_company = parser_company.find(text)
    if matches_company:
        # берём следующее слово после 'комания', считаем его названием компании
        company_name = matches_company.fact.first
        # Проверяем на синтаксическую связь со следующим словом
        doc_name = Doc(text)
        doc_name.segment(segmenter)
        doc_name.tag_morph(morph_tagger)
        doc_name.parse_syntax(syntax_parser)
        token_first = [token for token in doc_name.tokens if token.start == matches_company.tokens[1].span.start][0]
        token_second = [token for token in doc_name.tokens if token.start == matches_company.tokens[2].span.start][0]
        company_name = token_first.text
        if (token_first.head_id == token_second.id or
            token_first.id == token_second.head_id):
            # Если синтаксическая связь есть - добавляем это слово к первому слову названия
            company_name += ' ' + token_second.text
    return company_name


def insite_parcer(line):
    """ Принимаем на вход строку из датафрейма,
        возвращаем:
           Флаги:
            - менеджер поздоровался;
            - менеджер представился;
            - менеджер попращался
           Данные:
            - имя менеджера;
            - название компании.
    """
    # Инициализация ответов
    hello = np.nan
    introduce = np.nan
    manager_name = np.nan
    goodbye = np.nan
    company_name = np.nan
    # Часто встречающиеся поля входящей строки выделим в отдельные переменные
    text = line['text']
    dlg_id = line['dlg_id']
    
    
    # Если в строке не менеджер - то нечего проверять - сразу на выход
    if line['role'] != 'manager':
        return hello, introduce, manager_name,  goodbye, company_name
    
    
    # Если менеджер - начинаем с работать со строкой
    # Если начинаем работу с новым диалогом - создаём запись о новом диалоге в таблице результатов
    if not (dlg_id in results.index):
        results.loc[dlg_id] = {'hello': False,
                               'introduce': False,
                               'goodbye': False,
                              }
    
    # ПРИВЕТСТВИЕ
    if not(results.loc[dlg_id,'hello']):
        # Если менеджер в данном диалоге ещё не здоровался  - ищем
        if  parser_hello.find(text):
            hello = True
            results.loc[dlg_id,'hello'] = True
    
    
    # ПРЕДСТАВЛЕНИЕ МЕНЕДЖЕРА
    if not(results.loc[dlg_id,'introduce']):
        # Если менеджер в данном диалоге ещё не представлялся  - ищем
        introduce, manager_name = manager_name_extractor(text)
        results.loc[dlg_id,'introduce'] = introduce
        results.loc[dlg_id,'manager_name'] = manager_name
               
    
    # НАЗВАНИЕ КОМПАНИИ
    if pd.isnull(results.loc[dlg_id,'company_name']):
        # Если менеджер в данном диалоге ещё не называл компанию - ищем
        company_name = company_name_extractor(text)
        results.loc[dlg_id,'company_name'] = company_name
            
            
    # ПРОЩАНИЕ
    if not(results.loc[dlg_id,'goodbye']):
        # Если менеджер в данном диалоге ещё не прощался - ищем
        if  parser_goodbye.find(text):
            goodbye = True
            results.loc[dlg_id,'goodbye'] = True
    
            
    return hello, introduce, manager_name,  goodbye, company_name


# Создадим таблицу с итогами парсинга. Ещё она поможет не искать уже найденные сущности
results = pd.DataFrame(columns=['hello', 'introduce', 'manager_name', 'goodbye', 'company_name'])


# Список колонок с итогами парсинга
parce_columns = ['hello', 'introduce', 'manager_name', 'goodbye', 'company_name']
# Запускаем парсер
df[parce_columns] = [_ for _ in df.apply(insite_parcer, axis=1)]


# Добавляем колонку, в которй будет логическое "и" колонок Hello и Goodbye
results['hello_and_goodbye'] = results['hello'] & results['goodbye']


# Отображение таблицы результатов
print(results)


# Основной набор данных с не пустыми результатами поиска
print(df[df['hello'].notna() | 
     (df['introduce'].notna() & df['introduce']) | 
      df['manager_name'].notna() | 
      df['goodbye'].notna() | 
      df['company_name'].notna()]
     )
