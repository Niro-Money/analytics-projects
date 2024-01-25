from datetime import date
def age_calculator(s):
    if s==0:
        return 0
    else:
        year_of_birth=s%10000
        month_of_birth=(s%1000000)//10000
        Date_of_Birth=s//1000000
        Age=round((((date.today().year-year_of_birth)*12)+(date.today().month-month_of_birth)+((date.today().day-Date_of_Birth)/30))/12)
        return Age

