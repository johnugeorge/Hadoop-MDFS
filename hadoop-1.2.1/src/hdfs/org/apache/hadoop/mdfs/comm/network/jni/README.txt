gcc -c -I"/usr/lib/jvm/java-6-openjdk-amd64/include" galois.c jerasure.c reed_sol.c ReedSolomon.c


gcc -I"/usr/lib/jvm/java-6-openjdk-amd64/include" -o libReedSolomon.so -shared -Wl,-soname,ReedSolomon.so galois.o jerasure.o reed_sol.o ReedSolomon.o -lc


cp libReedSolomon.so to /usr/lib           
