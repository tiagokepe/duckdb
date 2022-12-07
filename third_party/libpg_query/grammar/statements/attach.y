/*****************************************************************************
 *
 * Attach Statement
 *
 *****************************************************************************/
AttachStmt:
				ATTACH opt_database Sconst AS ColId
				{
					PGAttachStmt *n = makeNode(PGAttachStmt);
					n->path = $3;
					n->name = $5;
					$$ = (PGNode *)n;
				}
		;

opt_database:	DATABASE									{}
			| /*EMPTY*/								{}
		;
