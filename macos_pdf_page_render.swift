import AppKit
import PDFKit

guard CommandLine.arguments.count >= 4 else {
    FileHandle.standardError.write(
        Data("usage: swift macos_pdf_page_render.swift <pdf> <page_1based> <out.png>\n".utf8)
    )
    exit(1)
}

let pdfPath = CommandLine.arguments[1]
guard let pageNum = Int(CommandLine.arguments[2]), pageNum >= 1 else {
    FileHandle.standardError.write(Data("page must be a positive integer\n".utf8))
    exit(2)
}
let outPath = CommandLine.arguments[3]

guard let doc = PDFDocument(url: URL(fileURLWithPath: pdfPath)) else {
    FileHandle.standardError.write(Data("unable to open PDF\n".utf8))
    exit(3)
}

if pageNum > doc.pageCount {
    FileHandle.standardError.write(
        Data("page \(pageNum) out of range (1...\(doc.pageCount))\n".utf8)
    )
    exit(4)
}

guard let page = doc.page(at: pageNum - 1) else {
    FileHandle.standardError.write(Data("unable to load page\n".utf8))
    exit(5)
}

let bounds = page.bounds(for: .mediaBox)
let maxDim: CGFloat = 1600
let w = bounds.width
let h = bounds.height
let scale = min(maxDim / max(w, h), 1.0)
let size = NSSize(width: w * scale, height: h * scale)
let img = page.thumbnail(of: size, for: .mediaBox)

guard let tiff = img.tiffRepresentation,
      let rep = NSBitmapImageRep(data: tiff),
      let png = rep.representation(using: .png, properties: [:]) else {
    FileHandle.standardError.write(Data("unable to encode PNG\n".utf8))
    exit(6)
}

do {
    try png.write(to: URL(fileURLWithPath: outPath))
} catch {
    FileHandle.standardError.write(Data("unable to write \(outPath): \(error)\n".utf8))
    exit(7)
}
