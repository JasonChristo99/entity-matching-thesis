from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.cosine import Cosine
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from strsimpy.damerau import Damerau

cosine = Cosine(2)
jarowinkler = JaroWinkler()
levenshtein = NormalizedLevenshtein()
damerau = Damerau()


def get_name_similarity(name1, name2):
    result = cosine_similarity(name1, name2)
    return result


def get_location_similarity(location1, location2):
    result = cosine_similarity(location1, location2)
    return result


def get_education_degree_similarity(degree1, degree2):
    result = cosine_similarity(degree1, degree2)
    return result


def get_education_university_similarity(university1, university2):
    result = cosine_similarity(university1, university2)
    return result


def get_education_year_similarity(year1, year2):
    year_sim: float
    try:
        diff = abs(int(year1) - int(year2))
        year_sim = 1 / (diff + 1)
    except:
        year_sim = cosine_similarity(year1, year2)

    result = year_sim
    return result


def get_working_experience_title_similarity(title1, title2):
    result = cosine_similarity(title1, title2)
    return result


def get_working_experience_company_similarity(company1, company2):
    result = cosine_similarity(company1, company2)
    return result


def get_working_experience_years_similarity(years1, years2):
    years_sim: float
    try:
        y11 = int(years1.split('-')[0])
        y12 = int(years1.split('-')[1])
        y21 = int(years2.split('-')[0])
        y22 = int(years2.split('-')[1])
        diff = abs(y11 - y12) + abs(y21 - y22)
        years_sim = 1 / (diff + 1)
    except:
        years_sim = cosine_similarity(years1, years2)

    result = years_sim
    return result


def get_skills_similarity(skill1, skill2):
    result = cosine_similarity(skill1, skill2)
    return result


def cosine_similarity(x: str, y: str):
    return cosine.similarity(x.lower(), y.lower())
