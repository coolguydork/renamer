import PDFKit

guard CommandLine.arguments.count == 3 else {
    FileHandle.standardError.write(
        Data("usage: swift macos_pdf_resave.swift <in.pdf> <out.pdf>\n".utf8)
    )
    exit(1)
}

let inPath = CommandLine.arguments[1]
let outPath = CommandLine.arguments[2]

guard let doc = PDFDocument(url: URL(fileURLWithPath: inPath)) else {
    FileHandle.standardError.write(Data("unable to open PDF\n".utf8))
    exit(2)
}

if doc.pageCount < 1 {
    FileHandle.standardError.write(Data("PDF has no pages\n".utf8))
    exit(3)
}

if !doc.write(to: URL(fileURLWithPath: outPath)) {
    FileHandle.standardError.write(Data("unable to write PDF\n".utf8))
    exit(4)
}
