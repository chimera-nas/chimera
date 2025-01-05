#pragma once

#include <stdint.h>

enum rb_color {
    RB_RED,
    RB_BLACK
};

struct rb_node {
    uint64_t        key;
    struct rb_node *left;
    struct rb_node *right;
    struct rb_node *parent;
    enum rb_color color;
};

struct rb_tree {
    struct rb_node *root;
    struct rb_node  nil;
};

static void
rb_node_init(
    struct rb_tree *tree,
    struct rb_node *node,
    uint64_t        key)
{
    node->key    = key;
    node->left   = &tree->nil;
    node->right  = &tree->nil;
    node->parent = &tree->nil;
    node->color  = RB_RED;
} /* rb_node_init */

static void
rb_left_rotate(
    struct rb_tree *tree,
    struct rb_node *x)
{
    struct rb_node *y = x->right;

    x->right = y->left;

    if (y->left != &tree->nil) {
        y->left->parent = x;
    }

    y->parent = x->parent;

    if (x->parent == &tree->nil) {
        tree->root = y;
    } else if (x == x->parent->left) {
        x->parent->left = y;
    } else {
        x->parent->right = y;
    }

    y->left   = x;
    x->parent = y;
} /* rb_left_rotate */

static void
rb_right_rotate(
    struct rb_tree *tree,
    struct rb_node *y)
{
    struct rb_node *x = y->left;

    y->left = x->right;

    if (x->right != &tree->nil) {
        x->right->parent = y;
    }

    x->parent = y->parent;

    if (y->parent == &tree->nil) {
        tree->root = x;
    } else if (y == y->parent->left) {
        y->parent->left = x;
    } else {
        y->parent->right = x;
    }

    x->right  = y;
    y->parent = x;
} /* rb_right_rotate */

static void
rb_insert_fixup(
    struct rb_tree *tree,
    struct rb_node *z)
{
    while (z->parent->color == RB_RED) {
        if (z->parent == z->parent->parent->left) {
            struct rb_node *y = z->parent->parent->right;
            if (y->color == RB_RED) {
                z->parent->color         = RB_BLACK;
                y->color                 = RB_BLACK;
                z->parent->parent->color = RB_RED;
                z                        = z->parent->parent;
            } else {
                if (z == z->parent->right) {
                    z = z->parent;
                    rb_left_rotate(tree, z);
                }
                z->parent->color         = RB_BLACK;
                z->parent->parent->color = RB_RED;
                rb_right_rotate(tree, z->parent->parent);
            }
        } else {
            struct rb_node *y = z->parent->parent->left;
            if (y->color == RB_RED) {
                z->parent->color         = RB_BLACK;
                y->color                 = RB_BLACK;
                z->parent->parent->color = RB_RED;
                z                        = z->parent->parent;
            } else {
                if (z == z->parent->left) {
                    z = z->parent;
                    rb_right_rotate(tree, z);
                }
                z->parent->color         = RB_BLACK;
                z->parent->parent->color = RB_RED;
                rb_left_rotate(tree, z->parent->parent);
            }
        }
    }
    tree->root->color = RB_BLACK;
} /* rb_insert_fixup */

static void
rb_tree_init(struct rb_tree *tree)
{
    tree->nil.color = RB_BLACK;
    tree->root      = &tree->nil;
} /* rb_tree_init */

static void
rb_tree_destroy(
    struct rb_tree *tree,
    void (         *free_payload )(
        struct rb_node *node,
        void           *private_data),
    void           *private_data)
{
    struct rb_node *node = tree->root;
    struct rb_node *parent;

    while (node != &tree->nil) {
        if (node->left != &tree->nil) {
            node = node->left;
            continue;
        }
        if (node->right != &tree->nil) {
            node = node->right;
            continue;
        }
        parent = node->parent;
        if (parent != &tree->nil) {
            if (parent->left == node) {
                parent->left = &tree->nil;
            } else {
                parent->right = &tree->nil;
            }
        }
        if (free_payload) {
            free_payload(node, private_data);
        }
        node = parent;
    }
} /* rb_tree_destroy */

static int
rb_tree_insert(
    struct rb_tree *tree,
    uint64_t        key,
    struct rb_node *node)
{
    struct rb_node *z = node;
    struct rb_node *y = &tree->nil;
    struct rb_node *x = tree->root;

    rb_node_init(tree, z, key);

    while (x != &tree->nil) {
        y = x;
        if (z->key < x->key) {
            x = x->left;
        } else if (z->key > x->key) {
            x = x->right;
        } else {
            return -1;  // Duplicate key
        }
    }

    z->parent = y;
    if (y == &tree->nil) {
        tree->root = z;
    } else if (z->key < y->key) {
        y->left = z;
    } else {
        y->right = z;
    }

    rb_insert_fixup(tree, z);
    return 0;
} /* rb_tree_insert */

static struct rb_node *
rb_tree_query_floor(
    struct rb_tree *tree,
    uint64_t        key)
{
    struct rb_node *node  = tree->root;
    struct rb_node *floor = NULL;

    while (node != &tree->nil) {
        if (node->key == key) {
            return node;
        }

        if (node->key > key) {
            node = node->left;
        } else {
            floor = node;
            node  = node->right;
        }
    }

    return floor;
} /* rb_tree_floor */

static struct rb_node *
rb_tree_query_exact(
    struct rb_tree *tree,
    uint64_t        key)
{
    struct rb_node *node = tree->root;

    while (node != &tree->nil) {
        if (key == node->key) {
            return node;
        }
        if (key < node->key) {
            node = node->left;
        } else {
            node = node->right;
        }
    }

    return NULL;
} /* rb_tree_query_exact */

static struct rb_node *
rb_tree_query_ceil(
    struct rb_tree *tree,
    uint64_t        key)
{
    struct rb_node *node = tree->root;
    struct rb_node *ceil = NULL;

    while (node != &tree->nil) {
        if (node->key == key) {
            return node;
        }

        if (node->key < key) {
            node = node->right;
        } else {
            ceil = node;
            node = node->left;
        }
    }

    return ceil;
} /* rb_tree_query_ceil */

static inline struct rb_node *
rb_tree_first(struct rb_tree *tree)
{
    struct rb_node *node = tree->root;

    while (node->left != &tree->nil) {
        node = node->left;
    }
    return node;
} /* rb_tree_first */

static inline struct rb_node *
rb_tree_next(
    struct rb_tree *tree,
    struct rb_node *node)
{
    if (node->right != &tree->nil) {
        node = node->right;
        while (node->left != &tree->nil) {
            node = node->left;
        }
        return node;
    }

    struct rb_node *parent = node->parent;
    while (parent != &tree->nil && node == parent->right) {
        node   = parent;
        parent = parent->parent;
    }
    return parent == &tree->nil ? NULL : parent;
} /* rb_next */

static void
rb_transplant(
    struct rb_tree *tree,
    struct rb_node *u,
    struct rb_node *v)
{
    if (u->parent == &tree->nil) {
        tree->root = v;
    } else if (u == u->parent->left) {
        u->parent->left = v;
    } else {
        u->parent->right = v;
    }
    v->parent = u->parent;
} /* rb_transplant */

static void
rb_delete_fixup(
    struct rb_tree *tree,
    struct rb_node *x,
    struct rb_node *parent)
{
    struct rb_node *w;

    while (x != tree->root && (x == &tree->nil || x->color == RB_BLACK)) {
        if (x == parent->left) {
            w = parent->right;
            if (w->color == RB_RED) {
                w->color      = RB_BLACK;
                parent->color = RB_RED;
                rb_left_rotate(tree, parent);
                w = parent->right;
            }
            if ((w->left == &tree->nil || w->left->color == RB_BLACK) &&
                (w->right == &tree->nil || w->right->color == RB_BLACK)) {
                w->color = RB_RED;
                x        = parent;
                parent   = x->parent;
            } else {
                if (w->right == &tree->nil || w->right->color == RB_BLACK) {
                    if (w->left != &tree->nil) {
                        w->left->color = RB_BLACK;
                    }
                    w->color = RB_RED;
                    rb_right_rotate(tree, w);
                    w = parent->right;
                }
                w->color      = parent->color;
                parent->color = RB_BLACK;
                if (w->right != &tree->nil) {
                    w->right->color = RB_BLACK;
                }
                rb_left_rotate(tree, parent);
                x      = tree->root;
                parent = &tree->nil;
            }
        } else {
            w = parent->left;
            if (w->color == RB_RED) {
                w->color      = RB_BLACK;
                parent->color = RB_RED;
                rb_right_rotate(tree, parent);
                w = parent->left;
            }
            if ((w->right == &tree->nil || w->right->color == RB_BLACK) &&
                (w->left == &tree->nil || w->left->color == RB_BLACK)) {
                w->color = RB_RED;
                x        = parent;
                parent   = x->parent;
            } else {
                if (w->left == &tree->nil || w->left->color == RB_BLACK) {
                    if (w->right != &tree->nil) {
                        w->right->color = RB_BLACK;
                    }
                    w->color = RB_RED;
                    rb_left_rotate(tree, w);
                    w = parent->left;
                }
                w->color      = parent->color;
                parent->color = RB_BLACK;
                if (w->left != &tree->nil) {
                    w->left->color = RB_BLACK;
                }
                rb_right_rotate(tree, parent);
                x      = tree->root;
                parent = &tree->nil;
            }
        }
    }
    if (x != &tree->nil) {
        x->color = RB_BLACK;
    }
} /* rb_delete_fixup */

static void
rb_tree_remove(
    struct rb_tree *tree,
    struct rb_node *z)
{
    struct rb_node *y = z;
    struct rb_node *x;
    struct rb_node *x_parent;
    enum rb_color   y_original_color = y->color;

    if (z->left == &tree->nil) {
        x        = z->right;
        x_parent = z->parent;
        rb_transplant(tree, z, z->right);
    } else if (z->right == &tree->nil) {
        x        = z->left;
        x_parent = z->parent;
        rb_transplant(tree, z, z->left);
    } else {
        y = z->right;
        while (y->left != &tree->nil) {
            y = y->left;
        }
        y_original_color = y->color;
        x                = y->right;
        x_parent         = y;

        if (y->parent == z) {
            x_parent = y;
        } else {
            x_parent = y->parent;
            rb_transplant(tree, y, y->right);
            y->right         = z->right;
            y->right->parent = y;
        }

        rb_transplant(tree, z, y);
        y->left         = z->left;
        y->left->parent = y;
        y->color        = z->color;
    }

    if (y_original_color == RB_BLACK) {
        rb_delete_fixup(tree, x, x_parent);
    }
} /* rb_tree_remove */