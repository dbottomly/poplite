SangerVarMask <- function()
{
    return(new("TableSchemaList", tab.list=list(probe_info=list(db.cols=c("probe_ind", "fasta_name", "probe_id", "align_status"),
                                db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "TEXT", "INTEGER", "TEXT"),
                                db.constr="CONSTRAINT probe_idx UNIQUE (fasta_name)",
                                dta.func=NULL, should.ignore=FALSE, foreign.keys=NULL),
                probe_align=list(db.cols=c("probe_align_id", "probe_chr", "probe_start", "probe_end", "probe_ind"),
                                db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "TEXT", "INTEGER", "INTEGER", "INTEGER"),
                                db.constr="CONSTRAINT geno_idx UNIQUE (probe_chr, probe_start, probe_end, probe_ind)",
                                dta.func=NULL, should.ignore=FALSE, foreign.keys=list(probe_info=list(local.keys="probe_ind", ext.keys="fasta_name"))),
                vcf_annot=list(db.cols=c("vcf_annot_id", "vcf_name", "type"),
                               db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "TEXT", "TEXT"),
                               db.constr="CONSTRAINT probe_idx UNIQUE (vcf_name, type)",
                               dta.func=NULL,
                               should.ignore=TRUE, foreign.keys=NULL),
                reference=list(db.cols=c("ref_id", "seqnames", "start", "end", "filter", "vcf_annot_id"),
                               db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "TEXT", "INTEGER", "INTEGER", "TEXT", "INTEGER"),
                               db.constr="CONSTRAINT ref_idx UNIQUE (seqnames, start, end, vcf_annot_id)",
                               dta.func=NULL, should.ignore=TRUE, foreign.keys=list(vcf_annot=list(local.keys="vcf_annot_id", ext.keys=c("vcf_name", "type")))),
                allele=list(db.cols=c("allele_id", "alleles", "allele_num", "ref_id"),
                            db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "TEXT", "INTEGER", "INTEGER"),
                            db.constr="CONSTRAINT alelle_idx UNIQUE (alleles, allele_num, ref_id)",
                            dta.func=NULL, should.ignore=TRUE, foreign.keys=list(vcf_annot=list(local.keys="vcf_annot_id", ext.keys=c("vcf_name", "type")),
                                                                                            reference=list(local.keys="ref_id", ext.keys=c("seqnames", "start", "end", "vcf_annot_id")))),
                genotype=list(db.cols=c("geno_id", "geno_chr", "allele_num","strain", "ref_id"),
                              db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "INTEGER", "TEXT", "INTEGER"),
                              db.constr="CONSTRAINT geno_idx UNIQUE (ref_id, strain, geno_chr, allele_num)",
                              dta.func=NULL, should.ignore=TRUE, foreign.keys=list(vcf_annot=list(local.keys="vcf_annot_id", ext.keys=c("vcf_name", "type")),
                                                                                                reference=list(local.keys="ref_id", ext.keys=c("seqnames", "start", "end", "vcf_annot_id")))),
                probe_to_snp=list(db.cols=c("probe_snp_id", "ref_id", "probe_align_id"),
                                  db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "INTEGER"),
                                  db.constr="CONSTRAINT p_s_idx UNIQUE (ref_id, probe_align_id)",
                                  dta.func=NULL, should.ignore=TRUE, foreign.keys=list(vcf_annot=list(local.keys="vcf_annot_id", ext.keys=c("vcf_name", "type")),
                                                                                                    reference=list(local.keys="ref_id", ext.keys=c("seqnames", "start", "end", "vcf_annot_id")),
                                                                                                    probe_align=list(local.keys="probe_align_id", ext.keys=c("probe_chr", "probe_start", "probe_end")))))))
}

Plethy <- function()
{
    return(new("TableSchemaList", tab.list=list()))
}