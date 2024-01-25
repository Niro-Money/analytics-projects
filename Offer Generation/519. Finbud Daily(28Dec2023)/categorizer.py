import logging
import re
from sklearn.base import BaseEstimator, TransformerMixin

logging.basicConfig(level=logging.INFO)


class categorizer(BaseEstimator, TransformerMixin):
    def __init__(self) -> None:
        super().__init__()

    def ageOnBureau(self, row):
        if row < 12:
            return "< 12"
        elif row <= 24:
            return "12-24"
        elif row <= 36:
            return "24-36"
        elif row <= 48:
            return "36-48"
        elif row <= 60:
            return "48-60"
        else:
            return "60+"

    def bankCards(self, row):
        if row == 0:
            return "0 cards"
        elif row == 1:
            return "1 card"
        elif row <= 5:
            return "2-5 cards"
        elif row <= 8:
            return "6-8 cards"
        elif row <= 10:
            return "9-10 cards"
        else:
            return "10+ cards"

    def mortgagebkt(self, row):
        if row == 0:
            return "0 trades"
        elif row == 1:
            return "1 trades"
        elif row <= 3:
            return "2-3 trades"
        elif row <= 4:
            return "3-4 trades"
        elif row <= 6:
            return "5-6 trades"
        else:
            return ">6 trades"

    def salarybkt(self, row):
        if row <= 20000:
            return "<20000"
        elif row <= 40000:
            return "20000-40000"
        elif row <= 60000:
            return "40000-60000"
        elif row <= 80000:
            return "60000-80000"
        elif row <= 100000:
            return "80000-100000"
        else:
            return ">100000"

    def worstRatingBkt(self, row):
        if row == 0:
            return "0 rating"
        elif row == 1:
            return "1 rating"
        elif row == 1.5:
            return "2 rating"
        elif row <= 3:
            return "2-3 rating"
        elif row <= 6:
            return "4-5 rating"
        else:
            return "5+ rating"

    def inquiryBkt(self, row):
        if row <= 0:
            return "0 inq"
        elif row <= 5:
            return "1-5 inq"
        elif row <= 10:
            return "6-10 inq"
        elif row <= 15:
            return "11-15 inq"
        elif row <= 20:
            return "16-20 inq"
        elif row <= 30:
            return "21-30 inq"
        elif row <= 50:
            return "31-50 inq"
        elif row <= 100:
            return "51-100 inq"
        else:
            return "100+ inq"

    def ageBkt(self, row):
        if row < 21:
            return "less than 21"
        elif row <= 30:
            return "21-30"
        elif row <= 40:
            return "31-40"
        elif row <= 50:
            return "41-50"
        elif row <= 57:
            return "51-57"
        else:
            return "57+"

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        k = {
            "MT01S": self.mortgagebkt,
            "AT20S": self.ageOnBureau,
            "BC02S": self.bankCards,
            "salary": self.salarybkt,
            "Calculated_Age": self.ageBkt,
        }
        for i in [i for i in X.columns if re.search("G310S", i)]:
            k[i] = self.worstRatingBkt
        for i in [i for i in X.columns if re.search("CV14", i)]:
            k[i] = self.inquiryBkt
        for key, func in k.items():
            # logging.info(f"categorizing {key}..")
            X[key].fillna(0, inplace=True)
            X[key] = X[key].astype(int)
            X[key] = X[key].apply(func)
        return X
