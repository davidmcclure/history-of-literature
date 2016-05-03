

from mpi4py import MPI

from hol.utils import grouper


if __name__ == '__main__':

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
        data = list(range(size))
        print(data)
    else:
        data = None

    data = comm.scatter(data, root=0)

    new_data = comm.gather(data**2, root=0)

    if rank == 0:
        print(new_data)
