# file-clone-validator
File migration validation tool

// todo: 进度条
// todo: 另一个问题是，如果是通过生成 temp_file 来 merge
// 在 filepath.Walk() 的时候可能会把 temp_file 也算进去
// 感觉需要写很多很 trick 的逻辑，来把 outDir/Temp 目录排除在外