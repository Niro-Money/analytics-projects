import pandas as pd
import numpy as np
import regex as re
import warnings
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)
warnings.filterwarnings('ignore')


tamil = ['appa', 'waran', 'samy', 'san', 'nnan',
         'perumal', 'raman', 'muthu', 'swamy', 'gan']
gujarathi = ['ben', 'bhai', 'bhen']
path = "dependencies/Language_prediction_dependency/"


def pin2lang(data):
    """Mapping.


    Parameters
    ----------
    data : pandas DataFrame

        DESCRIPTION.
        Function to map pincode to the language.

    Returns
    -------
    data : pandas DataFrame

        DESCRIPTION.
        appended dataframe with the language of the location.

    """
    pincodes = pd.read_csv(path + "india pincode final.csv")
    pincodes = pincodes[['pincode', 'Language']]
    pincodes['pincode'] = pincodes['pincode'].apply(lambda x: str(int(float(x))))
    data['pincode_Approved'] = data['pincode_Approved'].apply(lambda x: str(int(float(x))))
    data = data.merge(pincodes, left_on='pincode_Approved', right_on='pincode', how='left')
    return data


def matcher(data):
    """Mapping.


    Parameters
    ----------
    data : pandas DataFrame
        DESCRIPTION.
        Function to map all the tokens of name to the language.

    Returns
    -------
    data : pandas DataFrame
        DESCRIPTION.
        appended dataframe with all the languages of tokens.

    """
    mt_list = pd.read_csv(path+"mt_list.csv")
    mt_list = mt_list[['Surname', 'Mother Tongue']]
    mt_list['Mother Tongue'] = mt_list['Mother Tongue'].apply(lambda x: x.replace("]", "").replace("[", "").replace("'", "").strip().split(","))
    mt_list['Mother Tongue'] = mt_list['Mother Tongue'].apply(lambda x: ",".join(i for i in x))
    data['Final_name'] = data['Final_name'].astype(str).str.upper()
    data = data.join(data['Final_name'].astype(str).str.split(" ", expand=True))
    for i in data.columns:
        if type(i) == int:
            data = data.merge(mt_list, left_on=i, right_on='Surname', how='left')
            data.rename(columns={'Mother Tongue': 'MT'+str(i)}, inplace=True)
            data.rename(columns={i: 'token_'+str(i)}, inplace=True)
            data.drop(columns=['Surname'], inplace=True)
    data = null_cleaner(data)
    data = pin2lang(data)
    return data


def fun1(row):
    """Joining.


    Parameters
    ----------
    row : Series
        DESCRIPTION.
        This function takes in row of a Dataframe which contains matched languages of each token
        and joins them with a delimiter ",".

    Returns
    -------
    String
        DESCRIPTION.
        Returns the concatenated string which contains all matched languages of each token in the name.

    """
    col = [i for i in cols if (re.match(r"^MT", str(i))) and ((row[i] != 0) and (row[i] != "0"))]
    return ",".join(row[i] for i in col)


def fun2(row):
    """Joining.


    Parameters
    ----------
    row : Series
        DESCRIPTION.
        Function which takes row as an input and joins the matched language of either Tamil/Gujarati based on the tokens.

    Returns
    -------
    g : String
        DESCRIPTION.
        returns the joined string of all matched languages.

    """

    col = [i for i in cols if (re.match(r"^token", str(i))) and ((row[i] != 0) and (row[i] != "0"))]
    t = []
    for i in [row[j] for j in col if len(row[j]) >= 3]:
        i = str(i).lower()
        [t.append('Tamil') for j in tamil if re.search(r"{}$".format(j), i)]
        [t.append('Gujarati')
         for l in gujarathi if re.search(r"{}$".format(l), i)]
    g = ",".join(j for j in set(t))
    return g


def fun3(row):
    """Extracting.


    Parameters
    ----------
    row : Series
        DESCRIPTION.
        A function to get a single language for the given name.
        1. If only one language - same is considered.
        2. Taking the most frequent of all.
        3. If there are a list of languages, common language in both the list and of location is considered.
        4. If nothing matches - English is considered by default.

    Returns
    -------
    String
        DESCRIPTION.
        Single language.

    """
    ls = row['temp'].split(",")
    for i in row['temp1'].strip().split(","):
        ls.append(i)
    l = [i.strip().replace(" ", "") for i in ls if re.match(r"[a-zA-Z]+", i)]
    if len(l) != len(set(l)):
        return max(l, key=l.count)
    elif len(l) == 1:
        return l[0]
    else:
        if row['Language'] in l:
            return row['Language']
        else:
            return "English"


def null_cleaner(data):
    '''Removes Not Available(NA) values


    Parameters
    ----------
    data : pandas DataFrame
        DESCRIPTION.
        removes all null values by replacing them with "0".

    Returns
    -------
    data : pandas DataFrame
        DESCRIPTION.
        returns a null value free dataframe.

    '''
    data = data.replace(-999, np.nan)
    data = data.replace(np.inf, np.nan)
    data = data.replace(-np.inf, np.nan)
    data = data.fillna("0")
    return data


def Nlanguage(original):
    '''Main Function


    Parameters
    ----------
    data : pandas DataFrame
        DESCRIPTION.
        main function which contains all the methods that are to be excecuted.

    Returns
    -------
    data : pandas DataFrame
        DESCRIPTION.
        appends a column to the given data which contains the language.
    '''
    original['Member Reference'] = original['Member Reference'].astype(float).astype('int64').astype(str)
    data = original[['Member Reference', 'Final_name', 'pincode_Approved']]
    data = matcher(data)
    global cols
    cols = data.columns
    data['temp'] = data.apply(fun1, axis=1)
    data['temp1'] = data.apply(fun2, axis=1)
    data['language'] = data.apply(fun3, axis=1)
    data.drop_duplicates(subset='Member Reference', keep='first', inplace=True)
    temp = data[['Member Reference', 'language']]
    original = original.merge(temp, on='Member Reference', how='left')
    return original
