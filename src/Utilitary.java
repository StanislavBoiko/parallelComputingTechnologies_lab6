import java.util.Random;

public class Utilitary {
    public static final int FIRST_MATRIX_TAG = 0;
    public static final int FIRST_ROWS_NUM_TAG = 1;
    public static final int SECOND_MATRIX_TAG = 2;
    public static final int RES_TAG = 3;

    public static int[][] generateRandomMatrix(int len, int maxSize) {
        final Random random = new Random();
        final int[][] matrix = new int[len][len];

        for (int i = 0; i < len; i++) {
            for (int j = 0; j < len; j++) {
                matrix[i][j] = random.nextInt(maxSize) + 2;
            }
        }

        return matrix;
    }

    public static String matrixToString(int[][] matrix) {
        StringBuilder result = new StringBuilder();
        for (int[] row : matrix) {
            for (int val : row) {
                result.append(val).append(" ");
            }
            result.append("\n");
        }
        return result.toString();
    }
}
