import mpi.MPI;
import mpi.Request;

public class NonBlocking {

    private static final int MASTER = 0;
    private static final int MATRIXSIZE = 2000;
    private static final int MAXVAL = 100;

    public static void main(String[] args) {


        int[][] matrixA = Utilitary.generateRandomMatrix(MATRIXSIZE, MAXVAL);
        int[][] matrixB = Utilitary.generateRandomMatrix(MATRIXSIZE, MAXVAL);
        int[][] matrixC = new int[MATRIXSIZE][MATRIXSIZE];

        MPI.Init(args);

//        final int size = MPI.COMM_WORLD.Size();
        final int size = 6;
        final int rank = MPI.COMM_WORLD.Rank();
        final int workerCount = size - 1;

        if (rank == MASTER) {
            System.out.println("Started program with " + size + " processes!");


            final long start = System.currentTimeMillis();

            final int[] rows = new int[workerCount];
            final int[] offsets = new int[workerCount];
            final int averageRow = MATRIXSIZE / workerCount;
            int extra = MATRIXSIZE % workerCount;
            int offset = 0;

            for (int i = 1; i < workerCount + 1; i++) {
                final int rowsPerProcess = averageRow + (extra > 0 ? 1 : 0);

                rows[i - 1] = rowsPerProcess;
                offsets[i - 1] = offset;

                if (rowsPerProcess == 0)
                    break;

                MPI.COMM_WORLD.Isend(new int[]{rowsPerProcess}, 0, 1, MPI.INT, i, Utilitary.FIRST_ROWS_NUM_TAG);
                MPI.COMM_WORLD.Isend(matrixA, offset, rowsPerProcess, MPI.OBJECT, i, Utilitary.FIRST_MATRIX_TAG);
                MPI.COMM_WORLD.Isend(matrixB, 0, MATRIXSIZE, MPI.OBJECT, i, Utilitary.SECOND_MATRIX_TAG);

                extra--;
                offset += rowsPerProcess;
            }

            Request[] requestList = new Request[workerCount];

            for (int i = 1; i < workerCount + 1; i++) {
                final Request request = MPI.COMM_WORLD.Irecv(
                        matrixC, offsets[i - 1], rows[i - 1], MPI.OBJECT, i, Utilitary.RES_TAG);
                requestList[i - 1] = request;
            }


            Request.Waitall(requestList);

            final long finish = System.currentTimeMillis();



            System.out.println("matrix: " + MATRIXSIZE + "x" + MATRIXSIZE + "\n time: " + (finish - start) + " ms");


        } else {
            int[] rowsPerProcess = new int[1];

            final Request rowsRequest = MPI.COMM_WORLD.Irecv(rowsPerProcess, 0, 1,
                    MPI.INT, MASTER, Utilitary.FIRST_ROWS_NUM_TAG);
            rowsRequest.Wait();

            int[][] rowsA = new int[rowsPerProcess[0]][MATRIXSIZE];
            int[][] rowsB = new int[MATRIXSIZE][MATRIXSIZE];
            int[][] result = new int[rowsPerProcess[0]][MATRIXSIZE];

            final Request requestA = MPI.COMM_WORLD.Irecv(
                    rowsA, 0, rowsPerProcess[0], MPI.OBJECT, MASTER, Utilitary.FIRST_MATRIX_TAG);
            final Request requestB = MPI.COMM_WORLD.Irecv(
                    rowsB, 0, MATRIXSIZE, MPI.OBJECT, MASTER, Utilitary.SECOND_MATRIX_TAG);
            requestA.Wait();
            requestB.Wait();

            for (int i = 0; i < rowsA.length; i++) {
                for (int j = 0; j < rowsB[0].length; j++) {
                    int sum = 0;
                    for (int k = 0; k < rowsB.length; k++) {
                        sum += rowsA[i][k] * rowsB[k][j];
                    }
                    result[i][j] = sum;
                }
            }

            MPI.COMM_WORLD.Isend(result, 0, result.length, MPI.OBJECT, MASTER, Utilitary.RES_TAG);
        }

        MPI.Finalize();
//    }
    }
}