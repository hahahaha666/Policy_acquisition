def  one(o,**kwargs):
    print(o)
    print(kwargs)

if __name__ == '__main__':
    k={'1':'2'}
    one(1,**k)
