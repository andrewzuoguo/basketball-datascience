def implied_probability(odds, type: str="American") -> float:
    if type == "American":

        if odds < 0:
            return (-1*odds)/(-1*odds + 100)
        else:
            return 100/(odds + 100)

    elif type == "Decimal":
        return (1/odds)

    elif type == "Fractional":
        num = int(str(odds).split("/", 1)[0])
        denom = int(str(odds).split("/", 1)[1])

        return denom/(num+denom)