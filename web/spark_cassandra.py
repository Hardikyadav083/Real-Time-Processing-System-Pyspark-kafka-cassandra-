def edit_function(func):
    def wrapper(a,b):
        func(a,b)
        return a+b
    return wrapper


@edit_function
def sum_of_two_number(a,b):
    return a*b

result=sum_of_two_number(3,4)
print(result)