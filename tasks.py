

from invoke import task
from tables import IsDescription, StringCol, Int32Col, open_file


class TokenYearCount(IsDescription):
    token   = StringCol(50)
    year    = Int32Col()
    count   = Int32Col()


@task
def test():

    fh = open_file('test.h5', 'w', title='Test file.')

    group = fh.create_group('/', 'hol', 'History of literature')

    table = fh.create_table(group, 'counts', TokenYearCount)

    count = table.row

    for i in range(10000):
        count['token'] = 'token{0}'.format(i)
        count['year'] = i
        count['count'] = i
        count.append()

    fh.flush()

    for i in range(10000):
        for c in table.where('token == b"token'+str(i)+'" & year == {0}'.format(i)):
            print(i)
            c['count'] = 10
            c.update()

    fh.flush()
